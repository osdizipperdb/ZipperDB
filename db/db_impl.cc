// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
	// SqequenceNumber == long unsigned int 
	SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(nullptr),
      background_work_finished_signal_(&mutex_),
			//background_pmem_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(1000),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
			//background_pmem_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {
	//inserted for test
	for(int i =0;i<config::kPmemLevels;i++)
	{
		pmem_[i] = nullptr;
		imm_pmem_[i] = nullptr;
	}

  has_imm_.Release_Store(nullptr);

	pmem_size_factor = options_.pmem_size_factor;

	for(int i = 0 ; i < config::kPmemLevels; i++)
		has_imm_pmem_[i].Release_Store(nullptr);

	bool err_flag = false;

	for(int i = 0; i < config::kPoolNum;i++)
	{
		std::string dest = dbname_;
		std::string dest2 = dbname_;

		char buf[20];
		char buf2[20];

		snprintf(buf, sizeof(buf), "/level%d.set", i);
		snprintf(buf2, sizeof(buf2), "/level%d/000000.pmem", i);

		dest += buf;
		dest2 += buf2;

		pmem_arena_total_[i] = TOID_NULL(PmemArenaTotal);

		pop_[i] = nullptr;

		if(access(dest2.c_str(), 0) != 0)
		{
			pop_[i] = pmemobj_create(dest.c_str(), "PmemArenaTotal", 0, 0666);
			pmem_arena_total_[i] = POBJ_ROOT(pop_[i], PmemArenaTotal);
			for(int j = 0; j < config::kLevelPerPool; j++)
			{
				D_RW(pmem_arena_total_[i])->pmemarena_[j] = TOID_NULL(PmemArena);
				D_RW(pmem_arena_total_[i])->imm_pmemarena_[j] = TOID_NULL(PmemArena);
			}
		}
		else
		{
			pop_[i] = pmemobj_open(dest.c_str(), "PmemArenaTotal");
			pmem_arena_total_[i] = POBJ_ROOT(pop_[i], PmemArenaTotal);
		}

		if(pop_[i] == nullptr)
		{
			fprintf(stderr, "at pop[%d] ", i);
			perror("pop :");
			err_flag = true;
		}

	}

}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok
  
	while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }

	//inserted for test
	
	//inserted for test
	Status status;

	WritableFile* memfile = nullptr;
	uint64_t logfile_number;

	if(mem_ != nullptr)
	{
		logfile_number = 100;
		
		options_.env->NewWritableFile(LogFileName(dbname_, logfile_number), &memfile);

		log_ = new log::Writer(memfile);

		Iterator* mem_iter = mem_->NewIterator();

		mutex_.Unlock();

		mem_iter->SeekToFirst();
		if(mem_iter->Valid())
		{
			std::string cur_key;
			SequenceNumber seq;
			ParsedInternalKey mem_key;
			Slice mem_value;

			for(; mem_iter->Valid(); mem_iter->Next())
			{
				ParseInternalKey(mem_iter->key(), &mem_key);
				seq = mem_key.sequence;
				mem_value = mem_iter->value();

				WriteBatch batch;
				batch.Put(mem_key.user_key.ToString(), mem_value);
				WriteBatchInternal::SetSequence(&batch, seq);

				status = log_->AddRecord(WriteBatchInternal::Contents(&batch));
			}
		}
		mutex_.Lock();
	}

	if(imm_ != nullptr)
	{
		//WritableFile* memfile;
		if(memfile == nullptr)
		{
			logfile_number = 100;
			options_.env->NewWritableFile(LogFileName(dbname_, logfile_number), &memfile);
			log_ = new log::Writer(memfile);
		}

		Iterator* mem_iter = imm_->NewIterator();

		mutex_.Unlock();

		mem_iter->SeekToFirst();
		if(mem_iter->Valid())
		{
			std::string cur_key;
			SequenceNumber seq;
			ParsedInternalKey mem_key;
			Slice mem_value;

			for(; mem_iter->Valid(); mem_iter->Next())
			{
				ParseInternalKey(mem_iter->key(), &mem_key);
				seq = mem_key.sequence;
				mem_value = mem_iter->value();

				WriteBatch batch;
				batch.Put(mem_key.user_key.ToString(), mem_value);
				WriteBatchInternal::SetSequence(&batch, seq);

				status = log_->AddRecord(WriteBatchInternal::Contents(&batch));
			}
		}
		mutex_.Lock();
	}
	
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
	
	for(int i = 0; i < config::kPmemLevels; i++)
	{	
		if(pmem_[i] != nullptr )
		{
			pmem_[i]->SetAlive();
			pmem_[i]->Unref();
		}
		pmem_[i] = nullptr;
		if(imm_pmem_[i] != nullptr )
		{
			imm_pmem_[i]->SetAlive();
			imm_pmem_[i]->Unref();
		}
		imm_pmem_[i] = nullptr;

	}
	for(int i = 0; i < config::kPoolNum; i++)
	{
		if(pop_[i] != nullptr)
			pmemobj_close(pop_[i]);
	}

  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);
	
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            static_cast<int>(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }

  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
			
	
	if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
			
			if(type == kLogFile)
			{
				if(number == 100 ||
						(number >= 200 && number < 200+config::kPmemLevels)) 
					logs.push_back(number);
			}
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

	for (int i = config::kPoolNum - 1; i >= 0; i--)
	{
		for(int j = config::kLevelPerPool - 1; j >= 0; j--)
		{
			if(!TOID_IS_NULL(D_RW(pmem_arena_total_[i])->imm_pmemarena_[j]) 
					&& D_RW(D_RW(pmem_arena_total_[i])->imm_pmemarena_[j])->GetAllocHead())
			{
				D_RW(D_RW(pmem_arena_total_[i])->imm_pmemarena_[j])->constructor(pop_[i]);
				imm_pmem_[i*config::kLevelPerPool + j] 
					= new PMemTable(internal_comparator_, pop_[i], D_RW(pmem_arena_total_[i])->imm_pmemarena_[j]);
				imm_pmem_[i*config::kLevelPerPool + j]->Ref();
			}
			else if(!TOID_IS_NULL(D_RW(pmem_arena_total_[i])->imm_pmemarena_[j])
					&& !(D_RW(D_RW(pmem_arena_total_[i])->imm_pmemarena_[j])->GetAllocHead()))
			{
				POBJ_FREE(&(D_RW(pmem_arena_total_[i])->imm_pmemarena_[j]));
				D_RW(pmem_arena_total_[i])->imm_pmemarena_[j] = TOID_NULL(PmemArena);
			}
		
			if(!TOID_IS_NULL(D_RW(pmem_arena_total_[i])->pmemarena_[j]) 
					&& D_RW(D_RW(pmem_arena_total_[i])->pmemarena_[j])->GetAllocHead())
			{
				D_RW(D_RW(pmem_arena_total_[i])->pmemarena_[j])->constructor(pop_[i]);
				pmem_[i*config::kLevelPerPool + j] 
					= new PMemTable(internal_comparator_, pop_[i], D_RW(pmem_arena_total_[i])->pmemarena_[j]);
				pmem_[i*config::kLevelPerPool + j]->Ref();
			}
			else if(!TOID_IS_NULL(D_RW(pmem_arena_total_[i])->pmemarena_[j])
					&& !(D_RW(D_RW(pmem_arena_total_[i])->pmemarena_[j])->GetAllocHead()))
			{
				POBJ_FREE(&(D_RW(pmem_arena_total_[i])->pmemarena_[j]));
				D_RW(pmem_arena_total_[i])->pmemarena_[j] = TOID_NULL(PmemArena);
			}
		}
	}

	// Recover in the order in which the logs were generated
	std::sort(logs.begin(), logs.end()); 

  for (long long i = logs.size() - 1; i >= 0; i--) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
	
	MemTable* mem = nullptr;

  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
  	if (record.size() < 12) {
     reporter.Corruption(
				 record.size(), Status::Corruption("log record too small"));
     continue;
   }
		WriteBatchInternal::SetContents(&batch, record);
   if (log_number == 100) {
     if(mem == nullptr)
		 {
			 mem = new MemTable(internal_comparator_);
			 mem->Ref();
		 }
		 status = WriteBatchInternal::InsertInto(&batch, mem);
   }
	 /*else if(log_number >= 200 && log_number < 200+config::kPmemLevels)
	 {
		 bool first =true;

		 if(pmem == nullptr)
		 {
			 pmem = new PMemTable(internal_comparator_,dbname_, pmem_num_, &pmem_live_);
			 pmem_num_++;
			 pmem->Ref();
		 }
		 status = WriteBatchInternal::InsertInto(&batch, pmem, first);
		 if(first) first = false;
	 }*/
   
   MaybeIgnoreError(&status);
   if (!status.ok()) {
     break;
   }
   const SequenceNumber last_seq =
       WriteBatchInternal::Sequence(&batch) +
       WriteBatchInternal::Count(&batch) - 1;

   if (last_seq > *max_sequence) {
		 *max_sequence = last_seq;
	 }

  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    //assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
			//inserted for test
			status = WriteLevel0PMem(mem);
    }
    mem->Unref();
  }
  return status;
}

Status DBImpl::WriteLevel0PMem(MemTable* mem)
{
	mutex_.AssertHeld();

	const uint64_t start_micros = env_->NowMicros();
	Iterator* mem_iter = mem->NewIterator();

	if(pmem_[0] == nullptr)
	{
		//pmem does not exists
		//create new pmemarena_
		TOID(PmemArena) parena_;
		POBJ_ALLOC(pop_[0], &parena_, PmemArena, sizeof(PmemArena), NULL, NULL);

		D_RW(pmem_arena_total_[0])->pmemarena_[0] = parena_;
		D_RW(parena_)->SetAllocHead(false);
		D_RW(parena_)->constructor(pop_[0]);
		pmem_[0]  = new PMemTable(internal_comparator_, pop_[0], D_RW(pmem_arena_total_[0])->pmemarena_[0]);
		pmem_[0]->Ref();
	}
	pmem_[0]->Ref();

	PMemTable* pmem;
	pmem = pmem_[0];
	
	Status s;
	{
		mutex_.Unlock();
		mem_iter->SeekToFirst();

		if(mem_iter->Valid())
		{
			bool first = true;
			for(; mem_iter->Valid(); mem_iter->Next())
			{
				{
					ParsedInternalKey mem_key;
					ParseInternalKey(mem_iter->key(), &mem_key);
					Slice mem_value = mem_iter->value();
					uint64_t sequence = mem_key.sequence;

					pmem->Add(sequence, mem_key.type, mem_key.user_key.ToString(), mem_value, first);
					if(first)
						first = false;
				}
			}
		}

		mutex_.Lock();
	}

	delete mem_iter;
	pmem->Unref();
	
	return s;
}


void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  Status s = WriteLevel0PMem(imm_);

	VersionEdit edit;

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    
		//inserted for test
		DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }

}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();	
	
	bool PmemNeedCompaction = false;

	size_t pmem_size_ = options_.pmem_size;

	for (int level = 0; level < config::kPmemLevels - 1; level++)
	{
		if(pmem_[level] != nullptr)
		{
			if(pmem_[level]->ApproximateMemoryUsage() > pmem_size_ || (pmem_[level]->seek_count < 0))
			{

				PmemNeedCompaction = true;
				break;
			}
		}
		pmem_size_ *= pmem_size_factor;
	}

  if (background_compaction_scheduled_ 
			//|| background_pmem_compaction_scheduled_
			){
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr &&
             manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()
						 && !PmemNeedCompaction
						 ) {
    // No work to be done
  } else {
		background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

//inserted for test
void DBImpl::CompactPmemTable(const int level)
{ 
	
	Status status;
	
	Iterator* pmem_iter = imm_pmem_[level]->NewIterator();

	pmem_iter->SeekToFirst();

	assert(pmem_iter->Valid());

	
	if(level + 1 < config::kPmemLevels)
	{
		if(pmem_[level+1] == nullptr && ((level+1)%config::kLevelPerPool != 0))
		{
			mutex_.Lock();
			D_RW(pmem_arena_total_[(level+1)/config::kLevelPerPool])->pmemarena_[(level+1)%config::kLevelPerPool]
				= D_RW(pmem_arena_total_[(level)/config::kLevelPerPool])->imm_pmemarena_[(level)%config::kLevelPerPool];
			pmem_[level+1] = imm_pmem_[level];
			pmem_[level+1]->Ref();
			mutex_.Unlock();
		}
		else
		{
			if(pmem_[level + 1] == nullptr)
			{
				mutex_.Lock();
		
				TOID(PmemArena) parena_;
				POBJ_ALLOC(pop_[(level + 1)/(config::kLevelPerPool)], &parena_, PmemArena, sizeof(PmemArena), NULL, NULL);
	
				D_RW(pmem_arena_total_[(level + 1)/(config::kLevelPerPool)])->pmemarena_[(level+1)%(config::kLevelPerPool)] 
					= parena_;
				D_RW(parena_)->SetAllocHead(false);
				D_RW(parena_)->constructor(pop_[(level + 1)/config::kLevelPerPool]);
		
				pmem_[level+1] 
					= new PMemTable(internal_comparator_, pop_[(level+1)/(config::kLevelPerPool)], 
							D_RW(pmem_arena_total_[(level+1)/config::kLevelPerPool])->pmemarena_[(level+1)%config::kLevelPerPool]);
				pmem_[level+1]->Ref();
				mutex_.Unlock();
			}
			mutex_.Lock();
			pmem_[level+1]->Ref();
			mutex_.Unlock();
			status = PmemToPmemCompaction(level, pmem_iter);
			mutex_.Lock();
			pmem_[level+1]->Unref();
			mutex_.Unlock();
		}
	}

	PMemTable* tmp_imm_;
	mutex_.Lock();
	tmp_imm_ = imm_pmem_[level];
	imm_pmem_[level]->Unref();
	imm_pmem_[level] = nullptr;
	has_imm_pmem_[level].NoBarrier_Store(nullptr);
	D_RW(pmem_arena_total_[(level)/(config::kLevelPerPool)])->imm_pmemarena_[(level)%(config::kLevelPerPool)]
		= TOID_NULL(PmemArena);
	mutex_.Unlock();

	if (status.ok())
	{
		//done
	}
	else if (shutting_down_.Acquire_Load())
	{
		//Ignore compaction errors found during shutting down
	}

}

Status DBImpl::PmemToPmemCompaction(int level, Iterator* input_iter)
{

	PMemTable* pmem;

	pmem = pmem_[level+1];

	int counter_max = 4096;
	for(int i = 0 ; i < level; i++)
		counter_max *= pmem_size_factor;
	int interrupt_counter = counter_max;

	bool IsSeekCompaction = false;

	size_t current_level_size_ = options_.pmem_size;
	for(int i =0; i< level; i++)
		current_level_size_ *= pmem_size_factor;

	double current_level_score = 
		(double)imm_pmem_[level]->ApproximateMemoryUsage()/(double)current_level_size_;

	if(current_level_score  < 1.0)
	{
			IsSeekCompaction = true;
			counter_max = -1;
	}

	Status s;
	{
		input_iter->SeekToFirst();

		if(input_iter->Valid())
		{

			if(((level+1)%config::kLevelPerPool) != 0)
			{
				bool first = true;
				std::vector<AddressPair> merge_stack;
				//store where to modify
			
				std::string current_user_key;
				bool has_current_user_key = false;
				SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
				SequenceNumber smallest_snapshot;

				ParsedInternalKey pmem_key;
				Slice pmem_value;
				uint64_t sequence;

				if(snapshots_.empty())
					smallest_snapshot = versions_->LastSequence();
				else
					smallest_snapshot = snapshots_.oldest()->sequence_number();


				bool drop_flag = true;
				int prev_height = -1;

				for(; input_iter->Valid(); input_iter->Next())
				{
					interrupt_counter--;

					if((interrupt_counter < 0) && !shutting_down_.Acquire_Load())
					{
						if (has_imm_.NoBarrier_Load() != nullptr)
						{
							mutex_.Lock();
							if(imm_ != nullptr)
							{
								CompactMemTable();
								background_work_finished_signal_.SignalAll();
							}
							mutex_.Unlock();
							interrupt_counter = counter_max;
						}

						size_t pmem_size_ = options_.pmem_size;
						if(!IsSeekCompaction)
						{
							for (int i = 0; i < level; i++)
							{
								double pmem_score_ = -2;
	
								if(pmem_[i] != nullptr) 
								{
									pmem_score_ = (double)pmem_[i]->ApproximateMemoryUsage()/(double)pmem_size_;
	
									double next_pmem_score_ = -1;
									if (pmem_[i+1] != nullptr)
									{
										next_pmem_score_ =
											(double)pmem_[i+1]->ApproximateMemoryUsage() /
											((double)pmem_size_*pmem_size_factor);
									}

									if((pmem_score_ > 1.0) && (pmem_score_ > current_level_score)
											&& (pmem_score_ > next_pmem_score_))
									{
										mutex_.Lock();
										imm_pmem_[i] = pmem_[i];
										has_imm_pmem_[i].NoBarrier_Store(imm_pmem_[i]);
										D_RW(pmem_arena_total_[i/config::kLevelPerPool])->imm_pmemarena_[i%config::kLevelPerPool]
											= D_RO(pmem_arena_total_[i/config::kLevelPerPool])->pmemarena_[i%config::kLevelPerPool];
										D_RW(pmem_arena_total_[i/config::kLevelPerPool])->pmemarena_[i%config::kLevelPerPool]
											= TOID_NULL(PmemArena);
										pmem_[i] = nullptr;
										mutex_.Unlock();
										CompactPmemTable(i);
										interrupt_counter = counter_max;
										break;
									}

								}
								pmem_size_ *= pmem_size_factor;
							}
						}
					}
					
					bool drop = false;

					if(!ParseInternalKey(input_iter->key(), &pmem_key))
					{
						current_user_key.clear();
						has_current_user_key = false;
						last_sequence_for_key = kMaxSequenceNumber;
					}
					else
					{
						if(!has_current_user_key
								|| user_comparator()->Compare(pmem_key.user_key, Slice(current_user_key)) != 0)
						{
							 current_user_key.assign(pmem_key.user_key.data(), pmem_key.user_key.size());
							 has_current_user_key = true;
							 last_sequence_for_key = kMaxSequenceNumber;
						}
	
						if(last_sequence_for_key <= smallest_snapshot)
						{
							drop = true;
							drop_flag = true;
						}

						last_sequence_for_key = pmem_key.sequence;

						pmem_value = input_iter->value();
					
						sequence = pmem_key.sequence;

						if(!drop)
						{
							AddressPair Address;

							Address.target = input_iter->GetAddress();
							pmem->GetPrevAddress(input_iter->GetKeyOri(), Address.prev, first);

							int height = input_iter->GetHeight();

							if(!drop_flag)
							{
								TOID(char)* prev_node;
								bool* prev_merge = merge_stack.back().prev_merge;
								prev_node = merge_stack.back().prev;

								if(prev_node[0].oid.off == Address.prev[0].oid.off)
								{
									for(int h = 0; h < prev_height; h++)
									{
										Address.prev_merge[h] = false;
									}
									for(int h = prev_height; h < config::kSkiplistMaxHeight; h++)
										Address.prev_merge[h] = prev_merge[h];
								}
								else
								{
									for(int h = 0; h < config::kSkiplistMaxHeight; h++)
										Address.prev_merge[h] = true;
								}
							}
							else
							{
								for(int sl_h = 0; sl_h < config::kSkiplistMaxHeight; sl_h++)
									Address.prev_merge[sl_h] = true;
							}
							merge_stack.push_back(Address);
							prev_height = height;

							drop_flag = false;

							if(first)
								first = false;
						}
					}
				}
				
				for(;!merge_stack.empty();merge_stack.pop_back())
				{
					interrupt_counter--;

					if((interrupt_counter < 0) && !shutting_down_.Acquire_Load())
					{
						if (has_imm_.NoBarrier_Load() != nullptr)
						{
							mutex_.Lock();
							if(imm_ != nullptr)
							{
								CompactMemTable();
								background_work_finished_signal_.SignalAll();
							}
							mutex_.Unlock();
							interrupt_counter = counter_max;
						}

						size_t pmem_size_ = options_.pmem_size;
						if(!IsSeekCompaction)
						{
							for (int i = 0; i < level; i++)
							{
								double pmem_score_ = -2;
	
								if(pmem_[i] != nullptr) 
								{
									pmem_score_ = (double)pmem_[i]->ApproximateMemoryUsage()/(double)pmem_size_;
	
									double next_pmem_score_ = -1;
									if (pmem_[i+1] != nullptr)
									{
										next_pmem_score_ =
											(double)pmem_[i+1]->ApproximateMemoryUsage() /
											((double)pmem_size_*pmem_size_factor);
									}

									if((pmem_score_ > 1.0) && (pmem_score_ > current_level_score)
											&& (pmem_score_ > next_pmem_score_))
									{
										mutex_.Lock();

										imm_pmem_[i] = pmem_[i];
										has_imm_pmem_[i].NoBarrier_Store(imm_pmem_[i]);
										D_RW(pmem_arena_total_[i/config::kLevelPerPool])->imm_pmemarena_[i%config::kLevelPerPool]
											= D_RO(pmem_arena_total_[i/config::kLevelPerPool])->pmemarena_[i%config::kLevelPerPool];
										D_RW(pmem_arena_total_[i/config::kLevelPerPool])->pmemarena_[i%config::kLevelPerPool]
											= TOID_NULL(PmemArena);
										pmem_[i] = nullptr;
										mutex_.Unlock();
										CompactPmemTable(i);
										interrupt_counter = counter_max;
										break;
									}
								}
								pmem_size_ *= pmem_size_factor;
							}
						}
					}


					TOID(char) target = merge_stack.back().target;
					TOID(char)* prev = merge_stack.back().prev;
					bool* prev_merge = merge_stack.back().prev_merge;
					pmem->Merge(target,prev,prev_merge);
				}

				PmemArena* source = imm_pmem_[level]->GetArenaAddr();

				pmem->MergeArena(source);
			} //reallocate data by inserting
			else
			{
				bool first = true;
			
				counter_max *= pmem_size_factor;
				interrupt_counter = counter_max;
					
				//inserted for test
				std::string current_user_key;
				bool has_current_user_key = false;
				SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
				SequenceNumber smallest_snapshot;
	
				if (snapshots_.empty())
					smallest_snapshot = versions_->LastSequence();
				else
					smallest_snapshot = snapshots_.oldest()->sequence_number();
	
				ParsedInternalKey pmem_key;
				Slice pmem_value;
				uint64_t sequence;
	
				for (; input_iter->Valid(); input_iter->Next())
				{
					interrupt_counter--;

					if((interrupt_counter < 0) && !shutting_down_.Acquire_Load())
					{
						if (has_imm_.NoBarrier_Load() != nullptr)
						{
							mutex_.Lock();
							if(imm_ != nullptr)
							{
								CompactMemTable();
								background_work_finished_signal_.SignalAll();
							}
							mutex_.Unlock();
							interrupt_counter = counter_max;
						}
						
						size_t pmem_size_ = options_.pmem_size;
						if(!IsSeekCompaction)
						{
							for (int i = 0; i < level; i++)
							{
								double pmem_score_ = -2;
								if(pmem_[i] != nullptr)
								{
									pmem_score_ = (double)pmem_[i]->ApproximateMemoryUsage()/(double)pmem_size_;
							
									double next_pmem_score_ = -1;
									if (pmem_[i+1] != nullptr)
									{
										next_pmem_score_ =
											(double)pmem_[i+1]->ApproximateMemoryUsage() /
											((double)pmem_size_ * pmem_size_factor);
									}
			
									if((pmem_score_ > 1.0) && (pmem_score_ > current_level_score) 
											&& (pmem_score_ > next_pmem_score_))
									{
										mutex_.Lock();
										imm_pmem_[i] = pmem_[i];
										has_imm_pmem_[i].NoBarrier_Store(imm_pmem_[i]);
										D_RW(pmem_arena_total_[i/config::kLevelPerPool])->imm_pmemarena_[i%config::kLevelPerPool] 
											= D_RO(pmem_arena_total_[i/config::kLevelPerPool])->pmemarena_[i%config::kLevelPerPool];
										D_RW(pmem_arena_total_[i/config::kLevelPerPool])->pmemarena_[i%config::kLevelPerPool] 
											= TOID_NULL(PmemArena);

										pmem_[i] = nullptr;
										mutex_.Unlock();
		
										CompactPmemTable(i);
		
										interrupt_counter = counter_max;
										break;
									}
								}
		
								pmem_size_ *= pmem_size_factor;
							}
						}
					}
					bool drop = false;
	
					if(!ParseInternalKey(input_iter->key(), &pmem_key))
					{
						current_user_key.clear();
						has_current_user_key = false;
						last_sequence_for_key = kMaxSequenceNumber;
					}
					else
					{
						if(!has_current_user_key
								|| user_comparator()->Compare(pmem_key.user_key, Slice(current_user_key)) != 0)
						{
							 current_user_key.assign(pmem_key.user_key.data(), pmem_key.user_key.size());
							 has_current_user_key = true;
							 last_sequence_for_key = kMaxSequenceNumber;
						}
	
						if(last_sequence_for_key <= smallest_snapshot)
							drop = true;

						last_sequence_for_key = pmem_key.sequence;

						pmem_value = input_iter->value();
					
						sequence = pmem_key.sequence;

						if(!drop)
						{
							pmem->Add(sequence, pmem_key.type, pmem_key.user_key.ToString(), pmem_value, first);
							if(first)
								first = false;
						}
					}
				}
			}
		}
	}

	delete input_iter;

	return s;
}


void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

	//maybe write to level 0
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }
	
	for(int i = 0; i < config::kPmemLevels - 1; i++ )
	{
		if(imm_pmem_[i] != nullptr)
		{
			mutex_.Unlock();
			CompactPmemTable(i);
			mutex_.Lock();
			DeleteObsoleteFiles();
			return;
		}
	}

	mutex_.Unlock();

	size_t pmem_size_ = options_.pmem_size;
	
	double pmem_score_ = 0;
	int pmem_level_ = -1;
	int seek_pmem_level_ = -1;
	bool seek_compaction = false;

	for (int level = 0; level < config::kPmemLevels - 1; level++)
	{
		if(pmem_[level] != nullptr)
		{
			double tmp_score_ = (double)pmem_[level]->ApproximateMemoryUsage() / (double) pmem_size_;

			if (tmp_score_ > pmem_score_)
			{
				pmem_score_ = tmp_score_;
				pmem_level_ = level;
			}
			if(pmem_[level]->seek_count < 0)
			{
				if(seek_pmem_level_ < 0)
				{
					seek_pmem_level_ = level;
					seek_compaction = true;
				}
			}
		}
		pmem_size_ *= pmem_size_factor;
	}

	if(pmem_score_ > 1.0)
	{
		mutex_.Lock();
		imm_pmem_[pmem_level_] = pmem_[pmem_level_];
		has_imm_pmem_[pmem_level_].Release_Store(imm_pmem_[pmem_level_]);
		D_RW(pmem_arena_total_[pmem_level_/config::kLevelPerPool])->imm_pmemarena_[pmem_level_%config::kLevelPerPool] 
			= D_RO(pmem_arena_total_[pmem_level_/config::kLevelPerPool])->pmemarena_[pmem_level_%config::kLevelPerPool];
		D_RW(pmem_arena_total_[pmem_level_/config::kLevelPerPool])->pmemarena_[pmem_level_%config::kLevelPerPool] 
			= TOID_NULL(PmemArena);

		pmem_[pmem_level_] = nullptr;
		mutex_.Unlock();
		CompactPmemTable(pmem_level_);
	}
	else if(seek_compaction)
	{
		mutex_.Lock();
		imm_pmem_[seek_pmem_level_] = pmem_[seek_pmem_level_];
		has_imm_pmem_[seek_pmem_level_].Release_Store(imm_pmem_[seek_pmem_level_]);
		D_RW(pmem_arena_total_[seek_pmem_level_/config::kLevelPerPool])->imm_pmemarena_[seek_pmem_level_%config::kLevelPerPool]
			= D_RO(pmem_arena_total_[seek_pmem_level_/config::kLevelPerPool])->pmemarena_[seek_pmem_level_%config::kLevelPerPool];
		D_RW(pmem_arena_total_[seek_pmem_level_/config::kLevelPerPool])->pmemarena_[seek_pmem_level_%config::kLevelPerPool]
			= TOID_NULL(PmemArena);

		pmem_[seek_pmem_level_] = nullptr;
		mutex_.Unlock();
		CompactPmemTable(seek_pmem_level_);
	}

	mutex_.Lock();
	DeleteObsoleteFiles();

	return;

}


namespace {

struct IterState {
  port::Mutex* const mu;
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);
	uint64_t* const pmem;
	uint64_t* const imm_pmem;
	Version* const version GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, uint64_t* pmem, uint64_t* imm_pmem, Version* version)
      : mu(mutex), mem(mem), imm(imm), pmem(pmem), imm_pmem(imm_pmem), version(version)	
	{
	}
	~IterState()
	{
		free(pmem);
		free(imm_pmem);
	}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = (IterState*)(arg1);
	state->mu->Lock();
  state->mem->Unref();


  if (state->imm != nullptr) state->imm->Unref();

	for(int i = 0; i < config::kPmemLevels; i++)
	{
		if((PMemTable*)((state->pmem[i])) != nullptr)
			((PMemTable*)((state->pmem[i])))->Unref();
		if((PMemTable*)((state->imm_pmem[i])) != nullptr)
			((PMemTable*)((state->imm_pmem[i])))->Unref();
	}

  state->version->Unref();
  state->mu->Unlock();
  delete state;
	
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }

	uint64_t* pmem;
	uint64_t* imm_pmem;

	pmem = (uint64_t*)malloc(sizeof(uint64_t)* config::kPmemLevels);
	imm_pmem = (uint64_t*)malloc(sizeof(uint64_t)* config::kPmemLevels); 


	//inserted for test
	for(int i = 0; i < config::kPmemLevels; i++)
	{
		if (pmem_[i] != nullptr)
		{
			pmem[i] = (uint64_t)(void*)pmem_[i];
			pmem_[i]->Ref();
			list.push_back(pmem_[i]->NewIterator());
		}
		else
			pmem[i] = 0;
		if (imm_pmem_[i] != nullptr)
		{
			imm_pmem[i] = (uint64_t)(void*)imm_pmem_[i];
			imm_pmem_[i]->Ref();
			list.push_back(imm_pmem_[i]->NewIterator());
		}
		else
			imm_pmem[i] = 0;
	}
	
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, pmem, imm_pmem, versions_->current());

  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Update(const leveldb::WriteOptions& options,
											const leveldb::Slice& key,
											const leveldb::Slice& value)
{
	Status s;
	SequenceNumber snapshot = versions_->LastSequence();
	
	PMemTable* pmem[config::kPmemLevels];
	PMemTable* imm_pmem[config::kPmemLevels];

	bool pmem_ref[config::kPmemLevels];
	bool imm_pmem_ref[config::kPmemLevels];

	MemTable* mem = mem_;
	mem->Ref();
	MemTable* imm = imm_;
	if(imm != nullptr) imm->Ref();

	for(int i = 0; i < config::kPmemLevels; i++)
	{
		pmem[i] = pmem_[i];
		if(pmem[i] != nullptr)
		{
			pmem[i]->Ref();
			pmem_ref[i] = true;
		}
		else
			pmem_ref[i] = false;

		imm_pmem[i] = imm_pmem_[i];
		if(imm_pmem[i] != nullptr)
		{
			imm_pmem[i]->Ref();
			imm_pmem_ref[i] = true;
		}
		else
			imm_pmem_ref[i] = false;
	}

	std::string v;
	LookupKey lkey(key, snapshot);
	bool found = false;

	if (mem->Get(lkey, &v, &s))
		found = true;
  else if (imm != nullptr && imm->Get(lkey, &v, &s))
		found = true;
	else
	{
		for(int i = 0; i < config::kPmemLevels; i++)
		{
			if(pmem_ref[i])
			{
				if(pmem[i]->Get(lkey, &v, &s))
				{
					found = true;
					break;
				}
			}

			if(imm_pmem_ref[i])
			{
				if(imm_pmem[i]->Get(lkey, &v, &s))
				{
					found = true;
					break;
				}
			}
		}
	}

	mem->Unref();
	if (imm != nullptr) imm->Unref();
	for (int i = 0; i < config::kPmemLevels; i++)
	{
		if(pmem_ref[i])
			pmem[i]->Unref();
		if(imm_pmem_ref[i])
			imm_pmem[i]->Unref();
	}

	if(found)
	{
		s = Put(options, key, value);
		return Status::OK();
	}
	return Status::OK();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {

 
	Status s;
  SequenceNumber snapshot;

	PMemTable* pmem[config::kPmemLevels];
	PMemTable* imm_pmem[config::kPmemLevels];

	bool pmem_seek_several[config::kPmemLevels];

	for(int i = 0; i < config::kPmemLevels; i++)
	{
		pmem_seek_several[i] = false;
	}

	MutexLock l(&mutex_);

	snapshot = versions_->LastSequence();

	MemTable* mem = mem_;
	MemTable* imm = imm_;
	 
  mem->Ref();
  if (imm != nullptr) imm->Ref();

	for(int i = 0; i < config::kPmemLevels; i++)
	{
		pmem[i] = pmem_[i];
		if(pmem[i] != nullptr)
			pmem[i]->Ref();

		imm_pmem[i] = imm_pmem_[i];
		if(imm_pmem[i] != nullptr)
			imm_pmem[i]->Ref();
	}


  {
    mutex_.Unlock();

		LookupKey lkey(key, snapshot);

    if (mem->Get(lkey, value, &s)) {
			//found
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
			//found
    }
		else
		{
			for(int i = 0; i < config::kPmemLevels; i++)
			{
				if(pmem[i] != nullptr && pmem[i]->Get(lkey,value,&s))
				{
					break;
				}
				pmem_seek_several[i] = true;
				
				if(imm_pmem[i] != nullptr && imm_pmem[i]->Get(lkey, value, &s))
				{
					break;
				}
			} 
		}
		mutex_.Lock();
  }

  mem->Unref();
  if (imm != nullptr) imm->Unref();

	bool seek_compaction = false;
	for (int i = 0; i < config::kPmemLevels; i++)
	{
		if(pmem[i] != nullptr)
		{
			if(pmem_seek_several[i])
			{
				pmem[i]->seek_count--;
				if(pmem[i]->seek_count < 0)
				{
					seek_compaction = true;
				}
			}
			pmem[i]->Unref();
		}
		if(imm_pmem[i] != nullptr)
		{
			imm_pmem[i]->Unref();
		}
	}

	if(seek_compaction)
	{
		MaybeScheduleCompaction();
	}
	return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != nullptr
       ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {

  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  
	Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
	//writers_ -> Queue of writers
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  Status status = MakeRoomForWrite(my_batch == nullptr);
  
	uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

	//bench_mark
	//bench_end(&firstEnd);
	//bench_pause(&firstBegin, &firstEnd, &firstDiff);

  return status;
}


// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}


// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    }
		else if (
        allow_delay 
        //inserted for test
				&& pmem_[0] != nullptr
				&& (pmem_[0]->ApproximateMemoryUsage() >= (options_.pmem_size * 2))) {
				//versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } 
		else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      //Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    }//inserted for test
		else
		{
	/// 이부분 수정 필요	
			{
   	   //inserted for test
			
			// Attempt to switch to a new memtable and trigger compaction of old
      //assert(versions_->PrevLogNumber() == 1000);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
		
     	 MaybeScheduleCompaction();
    	}
		}
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
	WriteBatch batch;
  batch.Put(key, value);
 
	return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = nullptr;


  DBImpl* impl = new DBImpl(options, dbname);
	
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  
	if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();

    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();

    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(1000);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }

  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

	for(int i = 0; i < config::kPoolNum; i++)
	{
		std::string tmp;
		tmp = dbname;
		char buf[20];
		snprintf(buf, sizeof(buf), "/level%d.set", i);
		tmp += buf;

		pmempool_rm(tmp.c_str(), 0);
	}

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb

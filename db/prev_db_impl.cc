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
#include "db/pmemtable.h"
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

unsigned int dcounter;

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
      logfile_number_(0),
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
	
	//inserted for test
	for(int i = 0 ; i < config::kPmemLevels; i++)
		has_imm_pmem_[i].Release_Store(nullptr);

	//inserted for test
	counter = 0;
	ucounter = 0;
	dcounter = 0;
	mem_counter = 0 ;
	pmem_counter = 0;
	disk_counter = 0;
	foundtrace = false;
	pmem_avg_counter = 0;
	imm_avg_counter = 0;

	print_comp = true;


	for(int i = 0; i < config::kPmemLevels; i++)
		PmemDiff[i] = (struct timeval){0};

	for(int i= 0; i < config::kNumLevels; i++)
		DiskDiff[i] = (struct timeval){0};

	for(int i = 0; i < 5; i++)
		GetDiff[i] = (struct timeval){0};
			
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
	//inserted for test
	/*while (background_pmem_compaction_scheduled_)
	{
		background_pmem_work_finished_signal.Wait();
	}*/
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
	
	//inserted for test
	for(int i = 0; i < config::kPmemLevels; i++)
	{	
		if(pmem_[i] != nullptr && pmem_[i]->HasBoundaryKey())
		{
			pmem_[i]->Unref();
		}
		pmem_[i] = nullptr;
		if(imm_pmem_[i] != nullptr && pmem_[i]->HasBoundaryKey())
		{
			imm_pmem_[i]->Unref();
		}
		imm_pmem_[i] = nullptr;
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
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
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

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
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

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;

			//inserted for test
			status = WriteLevel0PMem(mem);
      //status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
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
      //status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

//inserted for test
//참고된 함수: 
//status dbimpl::writelevel0table		--db_impl.cc
//status dbimpl::write							--db_impl.cc
//status buildtable									--builder.cc
//TODO: pmem 새로 만들어 거기에 기존 pmem과 mem merge

Status DBImpl::WriteLevel0PMem(MemTable* mem)
{
	//printf("\nMove Mem -> Pmem 0\n");
	//printf("WriteLevel0PMem is called\n");
	
	mutex_.AssertHeld();

	const uint64_t start_micros = env_->NowMicros();
	Iterator* mem_iter = mem->NewIterator();

	if(pmem_[0] == nullptr || !(pmem_[0]->HasBoundaryKey()))
	{
		//pmem does not exists
		pmem_[0]  = new PMemTable(internal_comparator_);
		pmem_[0]->Ref();	
	}
	pmem_[0]->Ref();

	//inserte for test pmem
	PMemTable* pmem;
	pmem = pmem_[0];
	
	Status s;
	{
		mutex_.Unlock();
		mem_iter->SeekToFirst();

		if(mem_iter->Valid())
		{
			//inserted for test

			bool first = true;

			for(; mem_iter->Valid(); mem_iter->Next())
			{
				//MutexLock l(&mutex_);
				{
					ParsedInternalKey mem_key;
					ParseInternalKey(mem_iter->key(), &mem_key);
					Slice mem_value = mem_iter->value();
					uint64_t sequence = mem_key.sequence;

					WriteBatch batch;
					batch.Put(mem_key.user_key.ToString(), mem_value);	
					WriteBatchInternal::SetSequence(&batch, sequence);
					
					s = WriteBatchInternal::InsertInto(&batch, pmem, first);
					
					if(first)
						first = false;
				}
			}
		}

		mutex_.Lock();
	}

	/*
	//inserted for test
	Log(options_.info_log,
			"Compaction Memtable to Pmem[0] Smallest: %s, Largest: %s\n",
			(pmem_[0]->GetSmallestKey()).data(), (pmem_[0]->GetLargestKey()).data());
	*/

	delete mem_iter;
	pmem->Unref();

	return s;

	
}


Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  //meta file의 version number 설정
	//아마 지워도 됨
	meta.number = versions_->NewFileNumber();

	//pending_outputs_ -> set of table files to protect from deletion
	//set<uint64_t>로 되어있는걸로 보아 meta number의 set인듯함
	//역시 지워도 됨
	pending_outputs_.insert(meta.number);

	
	//memtable iterator 생성
  Iterator* iter = mem->NewIterator();

	//log 기록
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
		//memtable로 table build
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

	//logging
  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  
	//iterator 삭제
	delete iter;
	
	//아마 table로 옮기면 해당 meta number를 삭제(이미 썼으니까)
  pending_outputs_.erase(meta.number);

	
  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  Status s = WriteLevel0PMem(imm_);

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

	//inserted for test
  // Replace immutable memtable with the generated Table
  /*if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }*/

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    
		//inserted for test
		//DeleteObsoleteFiles();
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

	unsigned long int pmem_size_ = options_.pmem_size;

	for (int level = 0; level < config::kPmemLevels; level++)
	{
		if(pmem_[level] != nullptr)
		{
			if(pmem_[level]->ApproximateMemoryUsage() > pmem_size_)
			{
				PmemNeedCompaction = true;
				break;
			}
		}
		pmem_size_ *= options_.pmem_size_factor;
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
						 //inserted for test
						 //&& imm_pmem_[0] == nullptr
						 ) {
    // No work to be done
  } else {
//    if(!PmemNeedCompaction)
//			background_compaction_scheduled_ = true;
//		else
	//		background_pmem_compaction_scheduled_ = true;
	
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
	
	mutex_.AssertHeld();

	//printf("\nCompact MemTable at level : %d start\n", level);

	Status status;
	
	Compaction* c;


	unsigned long int pmem_size_ = options_.pmem_size;

	for(int lv = 0; lv < level; lv++)
		pmem_size_ *= 4;

	double current_pmem_score_ = 
		(double)imm_pmem_[level]->ApproximateMemoryUsage()/(double)pmem_size_;

	Iterator* pmem_iter = imm_pmem_[level]->NewIterator();

	pmem_iter->SeekToFirst();

	assert(pmem_iter->Valid());

	InternalKey smallestkey(imm_pmem_[level]->GetSmallestKey(), 
													imm_pmem_[level]->GetSmallestSequence(),
													imm_pmem_[level]->GetSmallestValueType());
	InternalKey largestkey(imm_pmem_[level]->GetLargestKey(),
													imm_pmem_[level]->GetLargestSequence(),
													imm_pmem_[level]->GetLargestValueType());

	c = versions_->PickPmemToFileCompaction(level, &smallestkey, &largestkey);

	
	if (c == nullptr){
		//nothing to do
	}
	//inputs are empty == no overlapping file in disk
	else if (c->num_input_files(0) == 0)
	{
		int compactiontype = -1;

		if(level + 1 < config::kPmemLevels)
		{
			if(pmem_[level+1] == nullptr)
			{
				//inserted for test
				pmem_[level+1] = new PMemTable(internal_comparator_);
				pmem_[level+1]->Ref();
				//compactiontype = 1;
				compactiontype = 2;
			}
			else
			{
				if(pmem_[level+1]->HasBoundaryKey() )
				{
					unsigned long int pmem_size_ = options_.pmem_size;
					for(int lv = 0; lv < level + 1; lv++)
						pmem_size_ *= 4;

					//inserted for test
					//pmem_size_  += options_.pmem_size;
				
					pmem_[level+1]->Ref();
					//if(imm_pmem_[level]->ApproximateMemoryUsage() 
					//		+ pmem_[level+1]->ApproximateMemoryUsage()
					//		<= pmem_size_)
					if(pmem_[level+1]->ApproximateMemoryUsage() < pmem_size_)
					{
						compactiontype = 2;
					}
					else
						compactiontype = 3;
					pmem_[level+1]->Unref();
				}
				else
				{
					pmem_[level+1]->Unref();
					pmem_[level+1] = nullptr;
					compactiontype =1;
				}
			}
		}
		else
			compactiontype = 3;

		//Pmem to Pmem move (just changing pointer)
		if(compactiontype == 1)
		{
			if(print_comp)
				printf("\nMove Pmem %d -> Pmem %d\n",level, level + 1);
			//
			
			//inserted for test
      //////////////////////////////////////////////////////////////////////
			//bench_start(&PmemBegin[level+1]);

			pmem_[level+1] = imm_pmem_[level];
			pmem_[level+1]->Ref();
	
			//bench_end(&PmemEnd[level+1]);

			//printf("\nPtP Pmem level : %d -> %d, time : ",level, level + 1);
			//bench_print(&PmemBegin[level+1], &PmemEnd[level + 1], &PmemDiff[level + 1]);
			//printf("\n");
			//////////////////////////////////////////////////////////////////////

			/*
			//inserted for test
			Log(options_.info_log, "Move pmem[%d]->pmem[%d], Smallest: %s, Largest: %s\n",
					level, level+1, 
					(pmem_[level+1]->GetSmallestKey()).data(), (pmem_[level+1]->GetLargestKey()).data());
			*/
			if(print_comp)
				printf("\nMove Pmem %d -> Pmem %d End\n", level, level + 1);

			delete pmem_iter;
		}
		//Pmem to Pmem Compact
		else if(compactiontype == 2)
		{
			if(print_comp)
				printf("\nCompact Pmem %d -> Pmem %d\n", level, level+1);
	
			//inserted for test
			//////////////////////////////////////////////////////////////////////
			//bench_start(&PmemBegin[level+1]);

			status = PmemToPmemCompaction(level, pmem_iter);

			//bench_end(&PmemEnd[level+1]);
			//printf("\nPwP Pmem level : %d -> %d, time : ", level, level+1);
			//bench_print(&PmemBegin[level+1],&PmemEnd[level+1],&PmemDiff[level+1]);
			//printf("\n");

			/*
			//inserted for test
			
			 Log(options_.info_log, 
					"Compact pmem[%d] with pmem[%d], Smallest: %s, Largest: %s\n", level, level+1, 
					(pmem_[level+1]->GetSmallestKey()).data(), (pmem_[level+1]->GetLargestKey()).data());
			*/
			//////////////////////////////////////////////////////////////////////////////////

			if(print_comp)
				printf("\nCompact Pmem %d -> Pmem %d End\n", level, level + 1);
		
		}
		//Pmem to Disk Move (no overlapping)
		else if(compactiontype == 3)
		{
		
			while(versions_->GetScore(level) > 1.0)
			{
				if(print_comp)
					printf("\nDisk Compaction called before PtD level %d\n", level);
				if(!(DiskCompaction(level)))
					break;

				//DiskCompaction은 해당 pmem과 같은 level의 disk compaction을 진행함
				//pmem과 overlapping이 없던 disk 에 compaction 이후에도
				//여전히 overlapping이 존재하지 않음
				//그렇기 때문에 새로 c를 만들 필요가 없음
			}
			//////////////////////////////////////////////////////////////////////////////////
			//inserted for test
			//bench_start(&DiskBegin[level]);

			if(print_comp)
				printf("\nMove Pmem %d -> Disk %d\n",level, level);

			CompactionState* compact = new CompactionState(c);

			Iterator* input = pmem_iter;

			pmem_iter->SeekToFirst();

			status = DoPmemCompaction(input, compact);
		
			CleanupCompaction(compact);
			c->ReleaseInputs();
			DeleteObsoleteFiles();

			//bench_end(&DiskEnd[level]);
			//printf("\nPtD Disk level : %d -> %d, time : ", level, level);
			//bench_print(&DiskBegin[level], &DiskEnd[level], &DiskDiff[level]);
			//printf("\n");

			/*
			//inserted for test
			Log(options_.info_log, "Move Pmem[%d] to Disk[%d] Smallest: %s, Largest: %s\n",
					level, level, (imm_pmem_[level]->GetSmallestKey()).data(), 
					(imm_pmem_[level]->GetLargestKey()).data());
			*/
			////////////////////////////////////////////////////////////////////////////////////

			if(print_comp)
				printf("\nMove Pmem %d -> Disk %d End\n", level, level);

		}
	}
	//Pmem with Disk Compaction
	else
	{

		int input0 = c->num_input_files(0);
		int input1 = c->num_input_files(1);
		double ScoreOfLevel = versions_->GetScore(level);

		int compactiontype = -1;

		if (versions_->GetScore(level) > 1.0)
		{
			c->ReleaseInputs();
			delete c;
			
			while(versions_->GetScore(level) > 1.0)
			{
				if(print_comp)
					printf("\nDisk Compaction called before PwD\n");
				//printf("\nScore of Disk: %lf\n", versions_->GetScore(level));
				if(!(DiskCompaction(level)))
					break;
			}

			//진행한 다음 새로운 c 만들기
			
			c = versions_->PickPmemToFileCompaction(level, &smallestkey, &largestkey);
	
			if (c == nullptr)
			{
				//nothing to do 
			}
			else if (c->num_input_files(0) == 0)
			{
				//can be happen
				//compaction type 1 never happen (no other pmem compaction proceed)
				//compaction type 2 can be happen (becuase of disk compaction)
				//compaction type 3 can be happen  

				if(level + 1 < config::kPmemLevels)
				{
					if(pmem_[level+1] == nullptr)
						compactiontype = 1;
					else
					{
						unsigned long int pmem_size_ = options_.pmem_size;
						for(int lv = 0; lv < level+1; lv++)
							pmem_size_ *= 4;
	
						//inserted for test
						//pmem_size_ += options_.pmem_size;

						pmem_[level+1]->Ref();
						//if (imm_pmem_[level]->ApproximateMemoryUsage()
						//		+ pmem_[level+1]->ApproximateMemoryUsage()
						//		<= pmem_size_)
						if(pmem_[level+1]->ApproximateMemoryUsage() <= pmem_size_)
							compactiontype = 2;
						else
							compactiontype = 3;
						pmem_[level+1]->Unref();
					}
				}
			}
		}

		int newinput0 = c->num_input_files(0);
		int newinput1 = c->num_input_files(1);

		if(compactiontype == 1)
		{
			if(print_comp)
				printf("\nMove Pmem %d -> Pmem %d\n",level, level+1);
			pmem_[level+1] = imm_pmem_[level];
			pmem_[level+1]->Ref();
		
			if(print_comp)
				printf("\nMove Pmem %d -> Pmem %d End\n", level, level+1);

			delete pmem_iter;
		}
		else if(compactiontype != 2)
		{
			if(print_comp)
				printf("\nCompact Pmem %d -> Disk %d\n", level, level);		
	
			//inserted for test
			/////////////////////////////////////////////////////////////////////////////////////
			//bench_start(&DiskBegin[level]);
	
			//insert for test
			int target = c->num_input_files(0);
			int numfiles = versions_->current()->NumFiles(level);

			//printf("\n\nAt Level %d, input files %d, total files %d\n\n", level, target, numfiles);

			CompactionState* compact = new CompactionState(c);
			//status = DoCompactionWork(compact);
			//do CompactionWork 내용
	
			Iterator* input;
	
			if (compactiontype == 3)
			{
				input = pmem_iter;
				pmem_iter->SeekToFirst();
			}
			else
				input = versions_->MakePmemToDiskIterator(c, pmem_iter);
	
			status = DoPmemCompaction(input, compact);
			
			CleanupCompaction(compact);
			c->ReleaseInputs();
			DeleteObsoleteFiles();
			
			//inserted for test
			//bench_end(&DiskEnd[level]);
			//printf("\nPwD Disk level : %d -> %d, time : ", level, level);
		  //bench_print(&DiskBegin[level], &DiskEnd[level], &DiskDiff[level]);
			//printf("\n");
			///////////////////////////////////////////////////////////////////////////////
			
			if(print_comp)
				printf("\nCompact Pmem %d -> Disk %d End\n", level, level);

			/*
			//inserted for test
			Log(options_.info_log, "Compact Pmem[%d] with Disk[%d], Smallest: %s, Largest: %s\n",
					level, level, (imm_pmem_[level]->GetSmallestKey()).data(),
					(imm_pmem_[level]->GetLargestKey()).data());

			*/

		}
		else if(compactiontype == 2)
		{
			if(print_comp)
				printf("\nPwD changed to PwP\n");
			status = PmemToPmemCompaction(level, pmem_iter);
			if(print_comp)
				printf("\nPwP End\n");
		}
	}

	delete c;

	imm_pmem_[level]->Unref();
	imm_pmem_[level] = nullptr;
	has_imm_pmem_[level].Release_Store(nullptr);

	if (status.ok())
	{
		//done
	}
	else if (shutting_down_.Acquire_Load())
	{
		//Ignore compaction errors found during shutting down
	}
	else
	{
		//log
	}

	//printf("PmemCompaction End level : %d\n\n",level);
}

Status DBImpl::PmemToPmemCompaction(int level, Iterator* input_iter)
{
	//assert(pmem_[level+1] != nullptr && pmem_[level + 1]->HasBoundaryKey());

	PMemTable* pmem;

	pmem = pmem_[level+1];
	pmem->Ref();

	Status s;
	{
		mutex_.Unlock();

		input_iter->SeekToFirst();

		if(input_iter->Valid())
		{
			bool first = true;

			//inserted for test
			Slice prev_key;

			for (; input_iter->Valid(); input_iter->Next())
			{
				MutexLock l(&mutex_);

				ParsedInternalKey pmem_key;
				ParseInternalKey(input_iter->key(), &pmem_key);

				Slice pmem_value = input_iter->value();
				uint64_t sequence = pmem_key.sequence;



				WriteBatch batch;
				batch.Put(pmem_key.user_key.ToString(), pmem_value);
				WriteBatchInternal::SetSequence(&batch, sequence);

				/*
				if(pmem_[level+1] == nullptr || !(pmem_[level+1]->HasBoundaryKey()))
				{
					pmem_[level+1] = new PMemTable(internal_comparator_);
					pmem_[level+1]->Ref();
					pmem_[level+1]->Ref();
					first = true;
				}*/

				s = WriteBatchInternal::InsertInto(&batch, pmem, first);

				prev_key = Slice(pmem_key.user_key.ToString());

				if(first)
					first = false;
			}
		}

		mutex_.Lock();
	}
	
	delete input_iter;
	pmem->Unref();

	return s;
}

Status DBImpl::DoPmemCompaction(Iterator* input, CompactionState* compact)
{

	//inserted for test

	const uint64_t start_micros = env_->NowMicros();
	int64_t imm_micros = 0;

	assert(compact->builder == nullptr);
	assert(compact->outfile == nullptr);

	if(snapshots_.empty())
	{
		compact->smallest_snapshot = versions_->LastSequence();
	}
	else
	{
		compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
	}

	mutex_.Unlock();

	Status status;

	input->SeekToFirst();

	ParsedInternalKey ikey;
	std::string current_user_key;
	bool has_current_user_key = false;
	SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

	bool first = true;


	for(; input->Valid() && !shutting_down_.Acquire_Load(); )
	{

		Slice key = input->key();

		if (compact->compaction->ShouldStopBefore(key) && compact->builder != nullptr)
		{
			status = FinishCompactionOutputFile(compact, input);
			if (!status.ok())
			{
				break;
			}

		}

		bool drop = false;

		if (!ParseInternalKey(key, &ikey))
		{
			current_user_key.clear();
			has_current_user_key = false;
			last_sequence_for_key = kMaxSequenceNumber;
		}
		else
		{
			if (!has_current_user_key ||
					user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0)
			{
				current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
				has_current_user_key = true;
				last_sequence_for_key = kMaxSequenceNumber;
			}

			if (last_sequence_for_key <= compact->smallest_snapshot)
			{
				drop = true;
			}
			else if (ikey.type == kTypeDeletion &&
					ikey.sequence <= compact->smallest_snapshot &&
					compact->compaction->IsBaseLevelForKey(ikey.user_key))
			{
				drop = true;
			}
			last_sequence_for_key = ikey.sequence;
		}

		if (!drop)
		{
			if (compact->builder == nullptr)
			{
				status = OpenCompactionOutputFile(compact);
				if(!status.ok())
				{
					break;
				}
			}

			if (compact->builder->NumEntries() == 0)
			{
				compact->current_output()->smallest.DecodeFrom(key);
			}
			compact->current_output()->largest.DecodeFrom(key);
			compact->builder->Add(key, input->value());

			if(first)
			{
				//printf("ikey = %s\n", ikey.user_key.ToString().c_str());
				first = false;
			}


			if (compact->builder->FileSize() >=
					compact->compaction->MaxOutputFileSize())
			{
				status = FinishCompactionOutputFile(compact, input);
				if (!status.ok())
				{
					break;
				}

				
				if (has_imm_.NoBarrier_Load() != nullptr)
				{
					const uint64_t imm_Start = env_->NowMicros();
					mutex_.Lock();
					if(imm_ != nullptr)
					{
						CompactMemTable();
						background_work_finished_signal_.SignalAll();
					}
					mutex_.Unlock();
					imm_micros += (env_->NowMicros() - imm_Start);
				}

				/*
				if(versions_->GetScore(compact->compaction->level()) > 1.0)
				{
					const uint64_t disk_Start =env_->NowMicros();
					mutex_.Lock();
					DiskCompaction(compact->compaction->level());
					mutex_.Unlock();
					imm_micros += (env_->NowMicros() - disk_Start);
				}*/

				for (int i = 0; i < compact->compaction->level(); i++)
				{
					if (has_imm_pmem_[i].NoBarrier_Load() != nullptr)
					{
						const uint64_t imm_Start = env_->NowMicros();
						mutex_.Lock();
						if(imm_pmem_[i] != nullptr)
						{
							CompactPmemTable(i);
							background_work_finished_signal_.SignalAll();
						}
						mutex_.Unlock();
						imm_micros += (env_->NowMicros() - imm_Start);
					}
				}
				
				unsigned long int pmem_size_ = options_.pmem_size;

				for (int i = 0; i < compact->compaction->level(); i++)
				{
					if (has_imm_pmem_[i].NoBarrier_Load() != nullptr)
					{
						const uint64_t imm_Start = env_->NowMicros();
						mutex_.Lock();
						if(imm_pmem_[i] != nullptr)
						{
							CompactPmemTable(i);
							background_work_finished_signal_.SignalAll();
						}
						mutex_.Unlock();
						imm_micros += (env_->NowMicros() - imm_Start);
					}



					double pmem_score_ = -2;
					if (pmem_[i] != nullptr && pmem_[i]->HasBoundaryKey())
					{
						pmem_score_ = (double)pmem_[i]->ApproximateMemoryUsage()/(double)pmem_size_;
					}
					
					double next_pmem_score_ = -1;
					if (pmem_[i+1] != nullptr)
					{
						next_pmem_score_ = 
							(double)pmem_[i+1]->ApproximateMemoryUsage()/((double)pmem_size_ * 4.0);
					}

					if ((pmem_score_ > 1) && (pmem_score_ > next_pmem_score_))
					{
						const uint64_t imm_Start = env_->NowMicros();
						mutex_.Lock();
						imm_pmem_[i] = pmem_[i];
						has_imm_pmem_[i].Release_Store(imm_pmem_[i]);

						//pmem_[i] = new PMemTable(internal_comparator_);
						//pmem_[i]->Ref();
						
						pmem_[i] = nullptr;

						CompactPmemTable(i);

						background_work_finished_signal_.SignalAll();
						mutex_.Unlock();
						imm_micros += (env_->NowMicros() - imm_Start);
					}
					pmem_size_ *= 4;
				}

			}
		}

		input->Next();
	}

	if(status.ok() && shutting_down_.Acquire_Load())
		status = Status::IOError("Deleting DB during compaction");
	if(status.ok() && compact->builder != nullptr)
	{
		status = FinishCompactionOutputFile(compact, input);
	}
	if (status.ok())
	{
		status = input->status();
	}

	delete input;

	input = nullptr;

	CompactionStats stats;
	stats.micros = env_->NowMicros() - start_micros - imm_micros;
	for (int i = 0; i < compact->compaction->num_input_files(0); i++)
	{
		stats.bytes_read += compact->compaction->input(0, i)->file_size;
	}
	for (size_t i = 0; i < compact->outputs.size(); i++)
		stats.bytes_written += compact->outputs[i].file_size;

	mutex_.Lock();

	//stats_[compact->compaction->level()].Add(stats);

	if (status.ok())
		status = InstallCompactionResultsPmemToDisk(compact);

	if (!status.ok())
		RecordBackgroundError(status);

	return status;

}

//Disk Compaction은 단발성
//Disk Compaction 동안은 다른 Compaction이 진행되지 않음
bool DBImpl::DiskCompaction(const int level)
{
	mutex_.AssertHeld();

	//printf("\nDisk Compaction %d -> %d is called\n", level, level+1);

	double disk_score_ = versions_->GetScore();

	bool is_compact_to_pmem = false;
	bool no_DTD = false;

	Compaction* c;

	//만들기
	//c = versions_->PickCompaction(level);

	//imm_pmem_[level+1]이 compaction 대기 중일때
	//왜 imm_pmem_의 smallest와 largest를 찾는거지?
	if(level + 1 < config::kPmemLevels && has_imm_pmem_[level+1].NoBarrier_Load() != nullptr)
	{
		InternalKey imm_pmem_smallest(imm_pmem_[level+1]->GetSmallestKey(),
				imm_pmem_[level+1]->GetSmallestSequence(),
				imm_pmem_[level+1]->GetSmallestValueType());
		InternalKey imm_pmem_largest(imm_pmem_[level+1]->GetLargestKey(),
				imm_pmem_[level+1]->GetLargestSequence(),
				imm_pmem_[level+1]->GetLargestValueType());
	
		c = versions_->PickCompaction(level, &imm_pmem_smallest, &imm_pmem_largest);
		no_DTD = true;
	}
	else
	{
		c = versions_->PickCompaction(level);
	}


	if(level + 1 < config::kPmemLevels)
		is_compact_to_pmem = true;

	Status status;
	if (c == nullptr) {
		//nothing to do	
		delete c;
		return false;
	}
	else if (!is_compact_to_pmem && c->IsTrivialMove())
	{
		//No pmem level and No overlapping file, move disk to disk
		assert(c->num_input_files(0) == 1);
		
		FileMetaData* f = c->input(0, 0);
		c->edit()->DeleteFile(c->level(), f->number);
		c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
				f->smallest, f->largest);
		status = versions_->LogAndApply(c->edit(), &mutex_);
		
		if (!status.ok())
			RecordBackgroundError(status);

		VersionSet::LevelSummaryStorage tmp;
		Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
				static_cast<unsigned long long>(f->number),
				c->level() + 1,
				static_cast<unsigned long long>(f->file_size),
				status.ToString().c_str(),
				versions_->LevelSummary(&tmp));
	}
	else if(is_compact_to_pmem)
	{
		
		bool can_move_disk_to_disk = false;

		if((c->num_input_files(0) == 1) && (c->num_input_files(1) == 0) && !no_DTD)
		{
			if(pmem_[c->level() + 1] == nullptr)
			{
				can_move_disk_to_disk = true;
			}
			else if(pmem_[c->level() + 1]->HasBoundaryKey())
			{
				InternalKey pmem_smallest(pmem_[c->level()+1]->GetSmallestKey(),
						pmem_[c->level()+1]->GetSmallestSequence(),
						pmem_[c->level()+1]->GetSmallestValueType());
				InternalKey pmem_largest(pmem_[c->level()+1]->GetLargestKey(),
						pmem_[c->level()+1]->GetLargestSequence(),
						pmem_[c->level()+1]->GetLargestValueType());

				//imm_pmem_ 조건도 추가 필요
				can_move_disk_to_disk = versions_->IsTrivialToDisk(c, pmem_smallest, pmem_largest);
			
			}
			else if(!(pmem_[c->level()+1]->HasBoundaryKey()))
			{
				pmem_[c->level()+1]->Unref();
				pmem_[c->level()+1] = nullptr;
				can_move_disk_to_disk = true;
			}
		}
		//input[1]에 무언가 있으면 can_move_disk_to_disk = false
		//하지만 can_move_disk_to_disk가 false인것의 모든 input[1]이 존재하는건 아님

		if(can_move_disk_to_disk)
		{
			assert(c->num_input_files(0) == 1);
			FileMetaData* f = c->input(0,0);

			if(print_comp)
				printf("\nAt DiskCompaction DiskCompaction DtD %d -> %d\n", level, level+1);

			/*
			//inserted for test
			Log(options_.info_log, "Move Disk[%d] to Disk[%d], Smallest: %s, Largest: %s\n",
					c->level(), c->level()+1,
					(f->smallest).user_key().data(), (f->largest).user_key().data());

			*/

			c->edit()->DeleteFile(c->level(), f->number);
			c->edit()->AddFile(c->level()+1, f->number, f->file_size, f->smallest, f->largest);
			status = versions_->LogAndApply(c->edit(), &mutex_);
			if(!status.ok())
			{
				RecordBackgroundError(status);
			}
			VersionSet::LevelSummaryStorage tmp;
			Log(options_.info_log, "Moved $%lld to level-%d %lld bytes %s: %s\n",
					static_cast<unsigned long long>(f->number),
					c->level() + 1,
					static_cast<unsigned long long>(f->file_size),
					status.ToString().c_str(),
					versions_->LevelSummary(&tmp));
			if(print_comp)
				printf("\nDiskCompaction DtD %d -> %d End\n", level, level+1);
		}
		else
		{
			if(print_comp)
				printf("DiskCompaction DwP %d -> %d\n", level, level +1);

			CompactionState* compact = new CompactionState(c);
			if (snapshots_.empty())
				compact->smallest_snapshot = versions_->LastSequence();
			else
				compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
		
			if(pmem_[c->level() + 1 ] == nullptr || !(pmem_[c->level() + 1]->HasBoundaryKey()))
			{
				pmem_[c->level() +1] = new PMemTable(internal_comparator_);
				pmem_[c->level() +1]->Ref();
			}

			PMemTable* pmem = pmem_[c->level() + 1];
			pmem->Ref();

			mutex_.Unlock();

			Iterator* input = versions_->MakeSingleInputIterator(compact->compaction);
			input->SeekToFirst();

			Status s;
			{
				if(input->Valid())
				{
					ParsedInternalKey input_key;
					Slice input_value;
					uint64_t sequence;

					std::string current_user_key;
					bool has_current_user_key = false;
					SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

					bool first = true;

					for(; input->Valid() && !shutting_down_.Acquire_Load(); input->Next())
					{
						// No more compaction during compaction
						// This function is designed for compact disk during pmem compaction
						//

						bool drop = false;

						if(!ParseInternalKey(input->key(), &input_key))
						{
							current_user_key.clear();
							has_current_user_key = false;
							last_sequence_for_key = kMaxSequenceNumber;
							//printf("\n\nerror occured at parsing internal key\n\n");
						}
						else
						{
							if(!has_current_user_key
									|| user_comparator()->Compare(input_key.user_key,
																								Slice(current_user_key)) != 0)
							{
								current_user_key.assign(input_key.user_key.data(), input_key.user_key.size());
								has_current_user_key = true;
								last_sequence_for_key = kMaxSequenceNumber;
							}

							if(last_sequence_for_key <= compact->smallest_snapshot)
								drop = true;

							last_sequence_for_key = input_key.sequence;

							input_value = input->value();
							sequence = input_key.sequence;
						}
						
						if(!drop)
						{
							WriteBatch batch;
							batch.Put(input_key.user_key.ToString(), input_value);
							WriteBatchInternal::SetSequence(&batch, sequence);

							s = WriteBatchInternal::InsertInto(&batch, pmem, first);


							if(first) {
							
								first = false; 
							}

						}
					}
				}

				/*
				//inserted for test
				FileMetaData* f = c->input(0,0);
				Log(options_.info_log,
						"Compact Disk[%d] with Pmem[%d]\nDisk smallest: %s, largest: %s\nPmem smallest: %s, largest: %s\n",
						c->level(), c->level()+1,
						(f->smallest).user_key().data(),
						(f->largest).user_key().data(),
						(pmem_[c->level()+1]->GetSmallestKey()).data(),
						(pmem_[c->level()+1]->GetLargestKey()).data());
				*/
			}

			if (s.ok() && shutting_down_.Acquire_Load())
				s = Status::IOError("Deletion DB during compaction");

			if (s.ok())
				s = input->status();

			delete input;
			input = nullptr;

			//pmem->Unref();

			mutex_.Lock();

			InstallCompactionResultsDiskToPmem(compact);

			pmem->Unref();
			//InstallCompactionResults(compact);

			CleanupCompaction(compact);

			c->ReleaseInputs();
			DeleteObsoleteFiles();

			if(print_comp)
				printf("\nDiskCompaction DwP %d -> %d End\n", level, level+1);
		}
	}
	else
	{
		//normal compaction process

		CompactionState* compact = new CompactionState(c);
		//수정필요
		status = DoDiskCompactionWork(compact);
		if(!status.ok())
		{
			RecordBackgroundError(status);
		}
		CleanupCompaction(compact);
		c->ReleaseInputs();
		DeleteObsoleteFiles();
	}

	delete c;

	if (status.ok()){
		//done
	} else if (shutting_down_.Acquire_Load()) {
		//Ignore Compaction Errors Found during Shutting down
	} else {
		Log(options_.info_log,
				"Compaction error: %s", status.ToString().c_str());
	}

	return true;
}

//inserted for test
Status DBImpl::DoDiskCompactionWork(CompactionState* compact)
{
	const uint64_t start_micros = env_->NowMicros();
	int64_t imm_micros = 0;
	/*
	 * Logging
	 */

	assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
	assert(compact->builder == nullptr);
	assert(compact->outfile == nullptr);
	if (snapshots_.empty())
		compact->smallest_snapshot = versions_->LastSequence();
	else
		compact->smallest_snapshot = snapshots_.oldest()->sequence_number();

	mutex_.Unlock();

	Iterator* input = versions_->MakeInputIterator(compact->compaction);
	input->SeekToFirst();
	Status status;
	ParsedInternalKey ikey;
	std::string current_user_key;
	bool has_current_user_key = false;
	SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

	for (; input->Valid() && !shutting_down_.Acquire_Load(); )
	{
		//Do not compact other imm/pmem
		
		Slice key = input->key();

		if (compact->compaction->ShouldStopBefore(key) 
				&& compact->builder != nullptr)
		{
			status = FinishCompactionOutputFile(compact, input);
			if (!status.ok())
			{
				break;
			}
		}

		bool drop = false;
		if (!ParseInternalKey(key, &ikey))
		{
			current_user_key.clear();
			has_current_user_key = false;
			last_sequence_for_key = kMaxSequenceNumber;
		}
		else
		{
			if(!has_current_user_key
					|| user_comparator()->Compare(ikey.user_key,
																				Slice(current_user_key)) != 0 )
			{
				current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
				has_current_user_key = true;
				last_sequence_for_key = kMaxSequenceNumber;
			}

			if (last_sequence_for_key <= compact->smallest_snapshot)
				drop = true;
			else if (ikey.type == kTypeDeletion 
						&& ikey.sequence <= compact->smallest_snapshot
						&& compact->compaction->IsBaseLevelForKey(ikey.user_key))
				drop = true;

			last_sequence_for_key = ikey.sequence;
		}

		if (!drop)
		{
			if(compact->builder == nullptr)
			{
				status = OpenCompactionOutputFile(compact);
				if (!status.ok())
					break;
			}

			if (compact->builder->NumEntries() == 0)
				compact->current_output()->smallest.DecodeFrom(key);

			compact->current_output()->largest.DecodeFrom(key);
			compact->builder->Add(key, input->value());

			if (compact->builder->FileSize()
					>= compact->compaction->MaxOutputFileSize())
			{
				status = FinishCompactionOutputFile(compact, input);
				if (!status.ok())
					break;
			}

		}

		input->Next();
	}
	
	if (status.ok() && shutting_down_.Acquire_Load())
		status = Status::IOError("Deleting DB during compaction");
	
	if (status.ok() && compact->builder != nullptr)
		status = FinishCompactionOutputFile(compact, input);

	if (status.ok())
			status = input->status();

	delete input;
	input = nullptr;

	CompactionStats stats;
	stats.micros = env_->NowMicros() - start_micros - imm_micros;

	for (int which = 0; which < 2; which++)
		for(int i = 0; i < compact->compaction->num_input_files(which); i++)
			stats.bytes_read += compact->compaction->input(which, i)->file_size;

	for (size_t i = 0; i < compact->outputs.size(); i++)
		stats.bytes_written += compact->outputs[i].file_size;

	mutex_.Lock();

	stats_[compact->compaction->level() + 1].Add(stats);

	if (status.ok())
		status = InstallCompactionResults(compact);

	if (!status.ok())
		RecordBackgroundError(status);
	
	VersionSet::LevelSummaryStorage tmp;
	
	/*
	 * Logging
	 */

	return status;
}

//compaction score를 조사하여 pmem와 disk중 어떤걸 compact할지 정해야 한다/
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

	//maybe write to level 0
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }
	
	for(int i = 0; i < config::kPmemLevels; i++ )
	{
		if(imm_pmem_[i] != nullptr)
		{
			CompactPmemTable(i);
			return;
		}
	}

	
	//inserted for test
	
	unsigned long int pmem_size_ = options_.pmem_size;
	
	double pmem_score_ = 0;
	int pmem_level_ = -1;

	for (int level = 0; level < config::kPmemLevels; level++)
	{
		if(pmem_[level] != nullptr && pmem_[level]->HasBoundaryKey())
		{
			double tmp_score_ = (double)pmem_[level]->ApproximateMemoryUsage() / (double) pmem_size_ ;
			if (tmp_score_ > pmem_score_)
			{
				pmem_score_ = tmp_score_;
				pmem_level_ = level;
			}
		}
		pmem_size_ *= 4;
	}

	double disk_score_ = versions_->GetScore();

	//printf("\nAt Background Compaction Pmem: %lf, Disk: %lf\n", pmem_score_, disk_score_);

	if((pmem_score_ > disk_score_) && (pmem_score_ > 1))
	{
		imm_pmem_[pmem_level_] = pmem_[pmem_level_];
		has_imm_pmem_[pmem_level_].Release_Store(imm_pmem_[pmem_level_]);

		//pmem_[pmem_level_] = new PMemTable(internal_comparator_);
		//pmem_[pmem_level_]->Ref();

		pmem_[pmem_level_] = nullptr;

		CompactPmemTable(pmem_level_);
		return;
	}

	
	/* 
	for (int level = 0; level < config::kPmemLevels; level++)
	{
		if(pmem_[level] != nullptr)
		{	
			if(pmem_[level]->ApproximateMemoryUsage() > pmem_size_)
			{
				//need compaction
				
				imm_pmem_[level] = pmem_[level];
				has_imm_pmem_[level].Release_Store(imm_pmem_[level]);

				pmem_[level] = new PMemTable(internal_comparator_);
				pmem_[level]->Ref();


				CompactPmemTable(level);
				return;
			}
		}
		pmem_size_ *= 3;
	}*/

	Compaction* c;


	//inserted for test
	//pmem으로 compact와 disk로 compact 구별
	bool is_compact_to_pmem = false;


	//manual_compaction_ 이 nullptr이 아니면 is_manual
	//manual_compaction_ 은 TEST_CompactRange에서 변경
	//TEST_CompactRange는 CompactRange에서 call 
  bool is_manual = (manual_compaction_ != nullptr);
  

	
	InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
		
		//inserted for test
		if(c->level() + 1 < config::kPmemLevels)
		{
			is_compact_to_pmem = true;
		}
  }
	
	//compaction할 table들이 c에 들어있다.

  Status status;
  if (c == nullptr) {
    // Nothing to do (nothing to do compaction)
  } else if (!is_manual 
			//inserted for test
			&& !is_compact_to_pmem 
			&& c->IsTrivialMove()) {
	
		
		// inserted for test
		if(print_comp)
			printf("\nTrivial move disk %d -> disk %d\n", c->level(), c->level()+1);

		//inserted for test
		///////////////////////////////////////////////////////////////////////////////
		//bench_start(&DiskBegin[c->level()+1]);

		//printf("Trivial move disk to disk\n");

		// No need to merge, or split, just move other level
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
		//이전 정보들 대신 새로운 레벨 정보만 넣음
    c->edit()->DeleteFile(c->level(), f->number);
		
		//inserted for test
		//if(c->level() == 10)
		//	c->edit()->AddFile(1, f->number,f->file_size,f->smallest,f->largest);
		//else
    	c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));

		//bench_end(&DiskEnd[c->level()+1]);
		//printf("\nDtD move at low Disk level : %d -> %d, time : ", c->level(), c->level() +1 );
		//bench_print(&DiskBegin[c->level()+1], &DiskEnd[c->level()+1], &DiskDiff[c->level()+1]);
		//printf("\n");
		//////////////////////////////////////////////////////////////////////////////////////
  }
	//inserted for test
	//disk to pmem compact	
	else if(is_compact_to_pmem)
	{
		//compact to pmem
		//



		bool can_move_disk_to_disk = false;

		if(!is_manual && c->IsTrivialMove())
		{
			if(pmem_[c->level()+1] == nullptr)
				can_move_disk_to_disk = true;
			else if(pmem_[c->level()+1]->HasBoundaryKey())
			{
				//printf("%lu\n", (unsigned long int)pmem_[c->level()+1]->GetSmallestValueType());
				InternalKey pmem_smallest(pmem_[c->level()+1]->GetSmallestKey(),
																	pmem_[c->level()+1]->GetSmallestSequence(),
																	pmem_[c->level()+1]->GetSmallestValueType());
				InternalKey pmem_largest(pmem_[c->level()+1]->GetLargestKey(),
																	pmem_[c->level()+1]->GetLargestSequence(),
																	pmem_[c->level()+1]->GetLargestValueType());

				can_move_disk_to_disk = versions_->IsTrivialToDisk(c, pmem_smallest, pmem_largest);
			}
			else if(!(pmem_[c->level()+1]->HasBoundaryKey()))
			{
				pmem_[c->level()+1]->Unref();
				pmem_[c->level()+1] = nullptr;
				if(c->num_input_files(1) == 0)
					can_move_disk_to_disk = true;
			}
		}

		if(can_move_disk_to_disk)
		{

			if(print_comp)	
				printf("\nMove disk %d -> disk %d\n", c->level(), c->level()+1);

			//inserted for test
			/////////////////////////////////////////////////////////////////////////////
			//bench_start(&DiskBegin[c->level()+1]);
			
			assert(c->num_input_files(0) == 1);
			FileMetaData* f = c->input(0,0);

			/*
			//inserted for test
			Log(options_.info_log, "Move Disk[%d] to Disk[%d], Smallest: %s, Largest: %s\n",
					c->level(), c->level()+1, 
					(f->smallest).user_key().data(), (f->largest).user_key().data());
			*/

			c->edit()->DeleteFile(c->level(), f->number);

			c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
					f->smallest, f->largest);
			status = versions_->LogAndApply(c->edit(), &mutex_);
			if (!status.ok())
			{
				RecordBackgroundError(status);
			}
			VersionSet::LevelSummaryStorage tmp;
			Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
					static_cast<unsigned long long>(f->number),
					c->level() + 1,
					static_cast<unsigned long long>(f->file_size),
					status.ToString().c_str(),
					versions_->LevelSummary(&tmp));

			//bench_end(&DiskEnd[c->level()+1]);
			//printf("\nDtD Disk level : %d -> %d, time : ",c->level(), c->level()+1);
			//bench_print(&DiskBegin[c->level()+1], &DiskEnd[c->level()+1], &DiskDiff[c->level()+1]);
			//printf("\n");
			/////////////////////////////////////////////////////////////////////////////////
			
			if(print_comp)
				printf("\nMove disk %d -> disk %d End\n", c->level(), c->level()+1);
		}
		else
		{
			//
			if(print_comp)
				printf("\nCompact disk %d -> pmem %d\n",c->level(), c->level()+1);
			//printf("disk to pmem is called\n");
			//inserted for test
			//////////////////////////////////////////////////////////////////////////////
			//bench_start(&PmemBegin[c->level()+1]);

			//printf("\nMove disk to pmem\n");
			CompactionState* compact = new CompactionState(c);
			if (snapshots_.empty())
			{
				compact->smallest_snapshot = versions_->LastSequence();
			}
			else
			{
				compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
			}
	
			if(pmem_[c->level() + 1] == nullptr || !(pmem_[c->level() + 1]->HasBoundaryKey()))
			{
				pmem_[c->level()+1] = new PMemTable(internal_comparator_);
				pmem_[c->level()+1]->Ref();
	
			}
			PMemTable* pmem = pmem_[c->level() + 1];
			pmem->Ref();

	
			mutex_.Unlock();

			//MakeInputIterator -> MakeSingleInputIterator로 수정 필요
			Iterator* input = versions_->MakeSingleInputIterator(compact->compaction);
			input->SeekToFirst();

			Status s;
			{
				if(input->Valid())
				{
			
					ParsedInternalKey input_key;
					Slice input_value;
					uint64_t sequence;

					std::string current_user_key;
					bool has_current_user_key = false;
					SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

					bool first = true;

					for(; input->Valid() && !shutting_down_.Acquire_Load(); input->Next())
					{
						if (has_imm_.NoBarrier_Load() != nullptr)
						{
							const uint64_t imm_start = env_->NowMicros();
							mutex_.Lock();
							if (imm_ != nullptr)
							{
								CompactMemTable();
								background_work_finished_signal_.SignalAll();
							}
							mutex_.Unlock();
							//imm_micros += (env_->NowMicros() - imm_start);
						}
						//수정필요
						//파일 하나 compact하는데 그렇게 오랜 시간이 걸리지 않음
						//+ background compaction이 돌아가는 동안 imm_pmem이 생길일이 있으려나?
						for(int i = 0; i< c->level(); i++)
						{
							if (has_imm_pmem_[i].NoBarrier_Load() != nullptr)
							{
								
								mutex_.Lock();
								if (imm_pmem_[i] != nullptr)
								{
									CompactPmemTable(i);
									background_work_finished_signal_.SignalAll();
								}
								mutex_.Unlock();
							}
						}
					
						bool drop = false;
	
						if(!ParseInternalKey(input->key(), &input_key))
						{
							current_user_key.clear();
							has_current_user_key = false;
							last_sequence_for_key = kMaxSequenceNumber;
						}
						else
						{
							if (!has_current_user_key 
									|| user_comparator()->Compare(input_key.user_key,
																								Slice(current_user_key)) != 0)
							{
								current_user_key.assign(input_key.user_key.data(), input_key.user_key.size());
								has_current_user_key = true;
								last_sequence_for_key = kMaxSequenceNumber;
							}
	
							if(last_sequence_for_key <= compact->smallest_snapshot)
							{
								drop = true;
							}
							
							last_sequence_for_key = input_key.sequence;
						
							input_value = input->value();
							sequence = input_key.sequence;
						}
	
						if(!drop)
						{
							WriteBatch batch;
							batch.Put(input_key.user_key.ToString(), input_value);
							WriteBatchInternal::SetSequence(&batch, sequence);
		

							s = WriteBatchInternal::InsertInto(&batch, pmem, first);

							if(first) {	first = false; }
	
						}
					}
				}

				/*
				//inserted for test
				FileMetaData* f = c->input(0,0);
				Log(options_.info_log, 
						"Compact Disk[%d] with Pmem[%d]\nDisk smallest: %s, largest: %s\nPmem smallest: %s, largest: %s\n",
						c->level(), c->level()+1,
						(f->smallest).user_key().data(),
						(f->largest).user_key().data(),
						(pmem_[c->level()+1]->GetSmallestKey()).data(),
						(pmem_[c->level()+1]->GetLargestKey()).data());
				*/
			}

			if (s.ok() && shutting_down_.Acquire_Load())
			{
				s = Status::IOError("Deleting DB during compaction");
			}
			if(s.ok())
			{
				s = input->status();
			}

			delete input;
			input = nullptr;	

			//pmem->Unref();

			//stats?
			//
		
			//CompactionStats stats;
			//stats.micros = env_->NowMicros() - start_micros - imm_micros;
			//stats.bytes_read += compact->compaction->input(0,0)->file_size;
			
			mutex_.Lock();

			InstallCompactionResultsDiskToPmem(compact);
			
			pmem->Unref();

			CleanupCompaction(compact);
			c->ReleaseInputs();
			DeleteObsoleteFiles();

			//bench_end(&PmemEnd[c->level()+1]);
			//printf("\nDwP Pmem level : %d -> %d, time : ",c->level(), c->level()+1);
			//bench_print(&PmemBegin[c->level()+1], &PmemEnd[c->level()+1], &PmemDiff[c->level()+1]);
			//printf("\n");
			/////////////////////////////////////////////////////////////////////////

			if(print_comp)
				printf("\nCompact disk %d -> pmem %d End\n",c->level(), c->level()+1);

		}
	}
	else {
		//normal compaction process
		if(print_comp)
			printf("\nCompact disk %d -> disk %d\n",c->level(), c->level()+1);
	


    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

	//printf("disk compaction is done\n\n");

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }


}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
		//여기서 outputfile 생성
    compact->outputs.push_back(out);
		
		//printf("output file number: %lu\n",file_number);
    
		mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);

	//WritableFile을 생성
	//PM을 넣으려면 이 부분도 고쳐야 함
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  

	//builder->Finish()에서 Table의 data block이후의 모든 data write
	if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResultsPmemToDisk(CompactionState* compact)
{
	mutex_.AssertHeld();

	//log
	//아마 여기서 문제
	
	compact->compaction->AddInputDeletions(compact->compaction->edit());
	const int level = compact->compaction->level();
	for (size_t i = 0; i < compact->outputs.size(); i++)
	{
		const CompactionState::Output& out = compact->outputs[i];
		compact->compaction->edit()->AddFile(level, 
				out.number, out.file_size, out.smallest, out.largest);
	}
	return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}


//inserted for test
Status DBImpl::InstallCompactionResultsDiskToPmem(CompactionState* compact)
{
	mutex_.AssertHeld();
	Log(options_.info_log, "Compacted %d@%d disk + %d pmem => %lld bytes",
			compact->compaction->num_input_files(0),
			compact->compaction->level(),
			compact->compaction->level()+1,
			static_cast<long long>(compact->total_bytes));

	compact->compaction->AddSingleInputDeletions(compact->compaction->edit());

	for (size_t i = 0; i < compact->outputs.size(); i++)
	{
		const CompactionState::Output& out = compact->outputs[i];
		//compact->compaction->edit()->AddFile(compact->compaction->level() + 1, 
		//		out.number, out.file_size, out.smallest, out.largest);
	}

	return versions_->LogAndApply(compact->compaction->edit(), &mutex_);

}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
/*
  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);
*/
  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }
	
	//inserted for test
	//printf("compact:smallest_snapshot %lu\n",compact->smallest_snapshot);


  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

	//inserted for test
	int max_pmem_level_ = config::kPmemLevels;

	if(compact->compaction->level() < max_pmem_level_)
		max_pmem_level_ = compact->compaction->level();

  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    
		if (has_imm_.NoBarrier_Load() != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }
		//inserted for test
		for(int i = 0; i < max_pmem_level_; i++)
		{
			if (has_imm_pmem_[i].NoBarrier_Load() != nullptr)
			{
				mutex_.Lock();
				if(
						//imm_ != nullptr && 
						(imm_pmem_[i] != nullptr))
				{
					CompactPmemTable(i);
					background_work_finished_signal_.SignalAll();
				}
				mutex_.Unlock();
			}
		}
    Slice key = input->key();

		if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
			//Compaction 마침
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
		
			//실질적 compaction
			if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
		/*
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
*/
    if (!drop) {

				

      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }

			/*
			//inserted for test
			if((count%100000 == 0)&&((compact->compaction->level()%2 == 1)))
			{
				printf("Output file name : %lu\n",compact->current_output()->number);
				printf("compaction at level %d: key-%s, value-%s",
						compact->compaction->level(),
						ikey.user_key.ToString().c_str(),
						input->value().ToString().c_str());
			}
			count++;
			*/
			
			//output에 넣기
      if (compact->builder->NumEntries() == 0) {
				//current_output()에 entrie가 없으면 smallest 로 들어감
        compact->current_output()->smallest.DecodeFrom(key);
      }
			//currnet_output()에 largest로 넣음
      compact->current_output()->largest.DecodeFrom(key);
      
			//key 넣기
			compact->builder->Add(key, input->value());
			

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();

	//inserted for test
	//if(compact->compaction->level() == 10)
	//	stats_[1].Add(stats);
	//else
		stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  /*
	Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));*/
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);
	//inserted for testt
	PMemTable** const pmem GUARDED_BY(mu);
	PMemTable** const imm_pmem GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, PMemTable** pmem, PMemTable** imm_pmem, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm), pmem(pmem), imm_pmem(imm_pmem) { }
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
	
  if (state->imm != nullptr) state->imm->Unref();
	
	//inserted for test
	for(int i = 0; i < config::kPmemLevels; i++)
	{
		if(state->pmem[i] != nullptr)
			state->pmem[i]->Unref();
		if(state->imm_pmem[i] != nullptr)
			state->imm_pmem[i]->Unref();
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

	//inserted for test
	for(int i = 0; i < config::kPmemLevels; i++)
	{
		if (pmem_[i] != nullptr && pmem_[i]->HasBoundaryKey())
		{
			list.push_back(pmem_[i]->NewIterator());
			pmem_[i]->Ref();
		}
		if (imm_pmem_[i] != nullptr && imm_pmem_[i]->HasBoundaryKey())
		{
			list.push_back(imm_pmem_[i]->NewIterator());
			imm_pmem_[i]->Ref();
		}
	}
	
	//list에 disk level 내용 추가
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, pmem_,imm_pmem_, versions_->current());
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

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
	Status d_s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;

	//일단 snapshot을 만든다 이부분에서 data를 얻어오는 것이므로 자세하게 볼 필요 있음
  if (options.snapshot != nullptr) {
		//새 snapshot을 만드는것
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;

	//inserted for test
	PMemTable* pmem[config::kPmemLevels];
	PMemTable* imm_pmem[config::kPmemLevels];

	bool pmem_ref[config::kPmemLevels];
	bool imm_pmem_ref[config::kPmemLevels];

	for(int i = 0; i < config::kPmemLevels; i++)
	{
		pmem[i] = pmem_[i];
		imm_pmem[i] = imm_pmem_[i];
		pmem_ref[i] = false;
		imm_pmem_ref[i] = false;
	}

	
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();


	//inserted for test
	for(int i = 0; i < config::kPmemLevels; i++)
	{
		if (pmem[i] != nullptr && pmem[i]->HasBoundaryKey())
		{
			pmem[i]->Ref();
			pmem_ref[i] = true;
			pmem_avg_counter++;
		}
		if (imm_pmem[i] != nullptr && imm_pmem[i]->HasBoundaryKey())
		{
			imm_pmem[i]->Ref();
			imm_pmem_ref[i] = true;
			imm_avg_counter++;
		}
	}

  bool have_stat_update = false;

	bool found = false;
	bool disk_found = false;
  
	Version::GetStats stats;

	//inserted for test
	stats.seek_file = nullptr;
	stats.seek_file_level = -1;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    //LookupKey는 key + sequence number Encoding
		LookupKey lkey(key, snapshot);

		/*
		if(counter%1000000 == 0)
		{
			unsigned long int pmem_size = options_.pmem_size;
			for (int i = 0; i< config::kPmemLevels; i++)
			{
				printf("\n\npmem[%d]: ", i);
				if(pmem_ref[i]){
					printf("exist, ");
					printf("score: %lf\n", (double)pmem[i]->ApproximateMemoryUsage()/(double)pmem_size );
				}
				else
				{
					if(pmem_[i] != nullptr && pmem_[i]->HasBoundaryKey())
						printf("it exists but can't see\n\n");
					else
						printf("does not exist\n\n");
				}

				printf("imm_pmem[%d]: ", i);
				if(imm_pmem_ref[i])
				{
					printf("exist, ");
					printf("score: %lf\n", (double)imm_pmem[i]->ApproximateMemoryUsage()/(double)pmem_size);
				}
				else
				{	
					if(imm_pmem_[i] != nullptr && imm_pmem_[i]->HasBoundaryKey())
						printf("it exists but can't see\n\n");
					else
						printf("does not exist\n\n");
				}
				pmem_size *=3;
			}
		}
		counter++;

		*/


    if (mem->Get(lkey, value, &s)) {
			found = true;
			
			//inserted for test
			mem_counter++;
      // Done

    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      
			found = true;

			//inserted for test
			mem_counter++;
		
    } //inserted for test
		else
		{
			int found_level = config::kNumLevels;
			for(int i = 0; i< config::kPmemLevels; i++)
			{
				//if(pmem[i] != nullptr && pmem[i]->HasBoundaryKey())
				if(pmem_ref[i])
				{
					/////////////////////////////////////////////////////////////////////
					//inserted for test
					bench_start(&PmemBegin[0]);
					

					if(user_comparator()->Compare(pmem[i]->GetSmallestKey(), key) <= 0 
							&& user_comparator()->Compare(pmem[i]->GetLargestKey(), key) >= 0)
					{
						ucounter++;
						if (pmem[i]->Get(lkey,value,&s))
						{
							found = true;

							found_level = i;
							
							///////////////////////////////////////////////////////////////////////
							//bench_end(&PmemEnd[0]);
							bench_pause(&PmemBegin[0], &PmemEnd[0], &PmemDiff[0]);
							pmem_counter++;
							///////////////////////////////////////////////////////////////////////
							break;
						}
						//ucounter++;
					}
				}
				//else if(imm_pmem[i] != nullptr && imm_pmem[i]->HasBoundaryKey())
				if(imm_pmem_ref[i])
				{
					if(user_comparator()->Compare(imm_pmem[i]->GetSmallestKey(), key) <= 0
							&& user_comparator()->Compare(imm_pmem[i]->GetLargestKey(), key) >= 0)
					{
						ucounter++;
						if (imm_pmem[i]->Get(lkey,value,&s))
						{
							found = true;

							found_level = i;
							///////////////////////////////////////////////////////////////////////
							//bench_end(&PmemEnd[0]);
							bench_pause(&PmemBegin[0], &PmemEnd[0], &PmemDiff[0]);
							pmem_counter++;
							///////////////////////////////////////////////////////////////////////
							break;
						}
						//ucounter++;
					}
				}

				//////////////////////////////////////////////////////////////////////
				bench_end(&PmemEnd[0]);
				bench_pause(&PmemBegin[0], &PmemEnd[0], &PmemDiff[0]);
				//////////////////////////////////////////////////////////////////////

				/*
				
				//////////////////////////////////////////////////////////////////////
				//inserted for test
				bench_start(&DiskBegin[0]);

				s = current->Get(i, options, lkey, value, &stats);
				have_stat_update = true;

				dcounter++;

				//bench_end(&DiskEnd[0]);
				bench_pause(&DiskBegin[0], &DiskEnd[0], &DiskDiff[0]);
				//////////////////////////////////////////////////////////////////////

				//s is found or it is deleted not not_found
				//then return the result about the key
				//and no need to see other level
				if(s.IsNotFound())
				{
					Status tmp;
					s = tmp;
					//dcounter++;
					continue;
				}
				else
				{
					found = true;

					//inserted for test
					disk_counter++;
					break;
				}
				
				 */
				
			}

			 
			//if(!found)
			{
				//for(int i = config::kPmemLevels; i < config::kNumLevels; i++)
				{
					//d_s = current->Get(i, options, lkey, value, &stats);
					bench_start(&DiskBegin[0]);
					d_s = current->Get(options,lkey, value, &stats, found_level);
					have_stat_update = true;
					
					//dcounter++;
					bench_pause(&DiskBegin[0], &DiskEnd[0], &DiskDiff[0]);
					//inserted for test
					if(d_s.ok() && !(d_s.IsNotFound()))
					{
						disk_found = true;
						disk_counter++;
					}
					
					//if(d_s.ok())
					//{
					//	disk_found = true;
					//	//break;
					//}
				}
			}
		}
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();

	//inserted for test
	for (int i = 0; i < config::kPmemLevels; i++)
	{
		if(pmem_ref[i])
			pmem[i]->Unref();
		if(imm_pmem_ref[i])
			imm_pmem[i]->Unref();
	}
  
	current->Unref();

	////////////////////////////////////////////////////////////////////////////////////////////
	counter++;

	if(counter%1000000 == 0)
	{
		printf("\n\nucounter: %u\ndcounter: %u\nMem_count: %u\nPmem_count: %u\nDisk_count: %u\nPmem: %ld.%06ld\nDisk: %ld.%06ld\n",
				ucounter, dcounter, mem_counter, pmem_counter, disk_counter,
				PmemDiff[0].tv_sec, PmemDiff[0].tv_usec,
				DiskDiff[0].tv_sec, DiskDiff[0].tv_usec);
				
		printf("Avg pmem cnt : %u\nAvg imm cnt : %u\n\n",pmem_avg_counter, imm_avg_counter);
	}
	////////////////////////////////////////////////////////////////////////////////////////////

	if(disk_found)
		return d_s;
	else if(found)
		return s;
	else
		return Status::NotFound(Slice());
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
	//inserted for test

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

  // May temporarily unlock and wait.
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

  return status;
}


/*
//inserted for test
WriteBatch* DBImpl::PMemBuildBatchGroup(Writer** writer_tmp)
{
	mutex_.AssertHeld();
	Writer* w_tmp = *writer_tmp;
	WriteBatch* result = w_tmp->batch;
	assert(result != nullptr);

	size_t size = WriteBatchInternal::ByteSize(w_tmp->batch);

	size_t max_size = 1 << 20;
	if (size <= (128<<10))
	{
		max_size = size + (128<<10);
	}

	*writer_tmp = w_tmp;
*/

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


/*
//What to do:
//	MakeRoomForWrite는 imm_->pmem write에만 관여하기
//  즉 imm_->pmem으로 write하는 것만 관여하고
//  pmem->disk로엔 관여하지 않음
Status DBImpl::MakeRoomForPmemWrite(
		int level //어디 level에 작성할지
		//Need to put something?
		//allow delay와 bool force처리를 어떻게?
		//일단 무조건 allow delay로 가정
		)
{
	//bool force = false;
	bool allow_delay = true;
	mutex_.AssertHeld();
	Status s;
	while(true)
	{
		if(!bg_error_.ok())
		{
			s = bg_error_;
			break;
		} 
		else if (
				allow_delay 
				&& versions_->NumLevelFiles(level) >= config::kL0_SlowdownWritesTrigger)
		//차후 kL0_SlowdonwWritesTrigger 수정 필요
		{
			mutex_.Unlock();
			env_->SleepForMicroseconds(1000);
			allow_delay = false;
			mutex_.Lock();
		} 
		else if ((pmem_[level] != nullptr)
				&& pmem_[level]->ApproximateMemoryUsage() <= options_.pmem_size)
		{
			//There is room in current pmem_
			break;
		}
		else if (imm_pmem_[level] != nullptr)
		{
			//pmem is being compacted so we wait;
			//log
			background_work_finished_signal_.Wait();
			//이 signal도 따로 하나 만들어야 하나?
		}
		else if (versions_->NumLevelFiles(level) >= config::kL0_StopWritesTrigger)
		{
			//Too many files in disk
			background_work_finished_signal_.Wait();
		}
		else if (pmem_[level] != nullptr)
		{
			uint64_t new_log_number = versions_->NewFileNumber();
			WritableFile* lfile = nullptr;
			s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
			if (!s.ok())
			{
				versions_->ReuseFileNumber(new_log_number);
				break;
			}
			delete log_;
			delete logfile_;
			logfile_ = lfile;
			logfile_number_ = new_log_number;
			log_ = new log::Writer(lfile);
			
			imm_pmem_[level] = pmem_[level];
			has_imm_pmem_.Release_Store(imm_pmem_[level]);

			pmem_[level] = new PMemTable(internal_comparator_);
			pmem_[level]->Ref();
			//force = false;
			MaybeScheduleCompaction();
		}
	}
	return s;
}
*/


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
				&& pmem_[0] != nullptr && pmem_[0]->HasBoundaryKey()
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
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    }//inserted for test
		else
		{
	/// 이부분 수정 필요	
			/*bool imm_pmem_exists = false;

			for( int i = 0; i < config::kPmemLevels; i++ )
			{
				if(imm_pmem_[i] != nullptr)
				{
					imm_pmem_exists = true;
				}
			}
			if (imm_pmem_exists)
				background_work_finished_signal_.Wait();
		
			else*/ if (
					//inserted for test
					pmem_[0] != nullptr && pmem_[0]->HasBoundaryKey() 
					&& pmem_[0]->ApproximateMemoryUsage() >= (options_.pmem_size * 3))
			{	
				//versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
  	    // There are too many level-0 files.
   	   
				Log(options_.info_log, "Too many L0 files; waiting...\n");
   	  	background_work_finished_signal_.Wait();
   	 	}
			else 
			{
   	   //inserted for test
			/*
			// Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
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
      force = false;   // Do not force another compaction if have room*/

				//inserted for test
				imm_ = mem_;
				has_imm_.Release_Store(imm_);
				//has_imm_pmem_.Release_Store(imm_pmem_[0]);
				mem_ = new MemTable(internal_comparator_);
				mem_->Ref();
				force = false;

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
  //printf("DB Put is called!\n");
	WriteBatch batch;
  batch.Put(key, value);
 
	/*
	if(DBcounter < 10)
		printf("key - %s\n, key - %s\n", key, key->data());
	DBcounter++;
	*/
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

			//inserted for test
			/*for(int i = 0; i < config::kPmemLevels; i++)
				impl->pmem_[i] = new PMemTable(impl->internal_comparator_);*/
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
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

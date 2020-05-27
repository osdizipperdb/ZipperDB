// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"

//inserted for test
#include "db/pmemtable.h"

#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const {
  return rep_.size();
}

//inserted for test
Status WriteBatch::PmemIterate(Handler* handler) const
{
	Slice input(rep_);
	if (input.size() < kHeader)
	{
		return Status::Corruption("malformed WriteBatch (too small)");
	}

	input.remove_prefix(kHeader);
	Slice key, value;
	int found = 0;
	while (!input.empty())
	{
		found++;
		char tag = input[0];
		input.remove_prefix(1);
		switch (tag)
		{
			case kTypeValue:
				if (GetLengthPrefixedSlice(&input, &key) &&
						GetLengthPrefixedSlice(&input, &value))
				{
					//printf("\nInsert value: key- %s\n",key.ToString().c_str());
					//usleep(50);
					handler->PmemPut(key, value);
				}
				else
				{
					return Status::Corruption("bad WriteBatch Put");
				}
				break;
			case kTypeDeletion:
				if (GetLengthPrefixedSlice(&input, &key))
				{
					handler->PmemDelete(key);
				}
				else
				{
					return Status::Corruption("bad WriteBatch Delete");
				}
				break;
			default:
				return Status::Corruption("unknown WriteBatch tag");
		}
	}
	if (found != WriteBatchInternal::Count(this))
	{
		return Status::Corruption("WriteBatch has wrong count");
	}
	else
	{
		return Status::OK();
	}
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {

					handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch &source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  SequenceNumber pmem_sequence_;

	MemTable* mem_;
	
	//inserted for test
	PMemTable* pmem_;

	bool first_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }

	//inserted for test
	virtual void PmemPut(const Slice& key, const Slice& value)
	{
		pmem_->Add(pmem_sequence_, kTypeValue, key, value, first_);
		//pmem_sequence_++;
	}

  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }

	virtual void PmemDelete(const Slice& key)
	{
		pmem_->Add(pmem_sequence_, kTypeDeletion, key, Slice(), first_);
		//pmem_sequence_++;
	}
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

//inserted for test
Status WriteBatchInternal::InsertInto(const WriteBatch* b, PMemTable* pmemtable, bool first)
{
	MemTableInserter inserter;
	inserter.pmem_sequence_ = WriteBatchInternal::Sequence(b);
	inserter.pmem_ = pmemtable;
	inserter.first_ = first;
	return b->PmemIterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb

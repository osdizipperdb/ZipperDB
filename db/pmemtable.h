// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//inserted for test

#ifndef STORAGE_LEVELDB_DB_PMEMTABLE_H_
#define STORAGE_LEVELDB_DB_PMEMTABLE_H_

#include <string>
#include <set>
#include <stdio.h>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/pmemskiplist.h"
#include "util/pmemarena.h"

//#define POOL_SIZE (52428800) //10MB
//#define POOL_SIZE (104857600) //100MB
//#define POOL_SIZE_ (1073741824) //1GB
//#define POOL_SIZE_ (5368709120) //5GB
//#define POOL_SIZE (10737418240) //10GB
//#define POOL_SIZE (16106127360) //15GB
#define POOL_SIZE_ (21474836480) //20GB
//#define POOL_SIZE (28991029248)
//#define POOL_SIZE (42949672960) //40GB
//#define POOL_SIZE (53687091200) //50GB
#define POOL_SIZE (70000000000) //65GB
//#define POOL_SIZE (85899345920)
//#define POOL_SIZE (config::kPmemSize)
//#define POOL_SIZE (107374182400) //100GB

namespace leveldb {

class InternalKeyComparator;
class PMemTableIterator;
class PmemArena;


class PMemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit PMemTable(const InternalKeyComparator& comparator, PMEMobjpool* pool, TOID(PmemArena) pmemarena);

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
			if(!alive_)
				destructor();
      delete this;
    }
  }

	void Unref_tmp() {
		--refs_;
	}

	void Unref_destructor() {
		if (refs_ <= 0)
		{
			if (!alive_)
				destructor();
			delete this;
		}
	}

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value,
					 bool first
					 );

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

	//bool HasBoundaryKey() { return has_smallest_largest; }

	//Slice GetSmallestKey() {	return smallest_;	}
	//SequenceNumber GetSmallestSequence() {	return smallest_sequence_; }
	//ValueType GetSmallestValueType() { return smallest_value_type_; }

	//Slice GetLargestKey()	{	return largest_; }
	//SequenceNumber GetLargestSequence() { return largest_sequence_; }
	//ValueType GetLargestValueType() { return largest_value_type_; }

	//void SetMinKey(Slice target, SequenceNumber seq, ValueType value_type);
	//void SetMaxKey(Slice target, SequenceNumber seq, ValueType value_type);

	int seek_count;

	int ReturnHeight()
	{
		return table_.ReturnHeight();
	}

	void GetPrevAddress(const char* target, TOID(char)* prev_node_, bool first);

	//void Merge(TOID(char) target, TOID(char) second, TOID(char)* prev);
	void Merge(TOID(char) target, TOID(char)* prev, bool* prev_merge);

	PmemArena* GetArenaAddr()
	{
		return D_RW(pmemarena_);
	}
	
	void MergeArena(PmemArena* source);

	void SetAlive()	{alive_ = true;}

 private:
  ~PMemTable();  // Private since only Unref() should be used to delete it

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };
  friend class PMemTableIterator;
  friend class PMemTableBackwardIterator;

  typedef PSkipList<const char*, KeyComparator> Table;

  KeyComparator comparator_;
  int refs_;
	TOID(PmemArena) pmemarena_;
  Table table_;

	std::string dest;
	PMEMobjpool *pop;
	bool alive_;

	void destructor();

  PMemTable(const PMemTable&);
  void operator=(const PMemTable&);

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

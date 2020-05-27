// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_PMEMARENA_H_
#define STORAGE_LEVELDB_UTIL_PMEMARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <libpmemobj.h>
#include <libpmempool.h>
#include "port/port.h"
#include "db/dbformat.h"

namespace leveldb {

	struct LinkedBlock;
	class PmemArena;
	class PmemArenaTotal;


	POBJ_LAYOUT_BEGIN(PmemArena);
	POBJ_LAYOUT_ROOT(PmemArena, PmemArenaTotal);
	POBJ_LAYOUT_TOID(PmemArena, PmemArena);
	POBJ_LAYOUT_TOID(PmemArena, LinkedBlock);
	POBJ_LAYOUT_END(PmemArena);

	struct LinkedBlock
	{
		TOID(char) block_addr_[1024];
		int block_num_;
		TOID(LinkedBlock) next_;

		void free_blocks()	
		{
			for(int i = 0; i < block_num_; i++)
				POBJ_FREE(&(block_addr_[i]));
		}
	};

class PmemArenaTotal
{
	public:
		TOID(PmemArena) pmemarena_[config::kLevelPerPool];
		TOID(PmemArena) imm_pmemarena_[config::kLevelPerPool];
};

class PmemArena {
 public:
  PmemArena();
  ~PmemArena();

	void constructor(PMEMobjpool *pool);
	void destructor();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  TOID(char) Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  TOID(char) AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
  }

	void BlockAppend(TOID(LinkedBlock) head, TOID(LinkedBlock) tail);
	void ClearBlock() { block_head_ = TOID_NULL(LinkedBlock); block_tail_ = TOID_NULL(LinkedBlock); }
	
	void MemoryUsageUpdate(size_t block_bytes);

	PMEMobjpool* GetPool() {return pop;}
	void SetAllocHead(bool val) {alloc_head_ = val;}
	bool GetAllocHead() {return alloc_head_;}
	TOID(char) GetHeadAddr() {return head_addr_;}

	int max_height_;

	TOID(LinkedBlock) block_head_;
	TOID(LinkedBlock) block_tail_;

 private:
  TOID(char) AllocateFallback(size_t bytes);
  TOID(char) AllocateNewBlock(size_t block_bytes);

  // Allocation state
  TOID(char) alloc_ptr_;
  size_t alloc_bytes_remaining_;

  port::AtomicPointer memory_usage_;

	PMEMobjpool *pop;

	TOID(char) head_addr_;
	bool alloc_head_;

  // No copying allowed
  PmemArena(const PmemArena&);
  void operator=(const PmemArena&);
};

inline TOID(char) PmemArena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    TOID(char) result = alloc_ptr_;
    alloc_ptr_.oid.off += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
	//return AllocateNewBlock(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/pmemarena.h"
#include <assert.h>

namespace leveldb {

static const int kBlockSize = 67108864;
struct LinkedBlock;

PmemArena::PmemArena(){}

void PmemArena::constructor(PMEMobjpool *pool)
{
	if(!alloc_head_)
	{
		memory_usage_.NoBarrier_Store(0);
		alloc_bytes_remaining_ = 0;
		block_head_ = TOID_NULL(LinkedBlock);
		block_tail_ = TOID_NULL(LinkedBlock);
	}
	pop = pool;
}

void PmemArena::destructor()
{

	TOID(LinkedBlock) tmp;
	TOID(LinkedBlock) next;
	tmp = block_head_;

	while(!TOID_IS_NULL(tmp))
	{
		next = D_RW(tmp)->next_;
		D_RW(tmp)->free_blocks();
		POBJ_FREE(&tmp);
		tmp = next;
	}
	block_head_ = TOID_NULL(LinkedBlock);
	block_tail_ = TOID_NULL(LinkedBlock);
	alloc_ptr_ = TOID_NULL(char);
	pop = NULL;
	head_addr_ = TOID_NULL(char);
	alloc_head_ = false;
}

PmemArena::~PmemArena() {
	destructor();
}

TOID(char) PmemArena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    TOID(char) result = AllocateNewBlock(bytes);
    return result;
  }
  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  TOID(char) result = alloc_ptr_;
  alloc_ptr_.oid.off += bytes;
  alloc_bytes_remaining_ -= bytes;

  return result;
}

TOID(char) PmemArena::AllocateAligned(size_t bytes) {
	{
		const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
 	 assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
 	 size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_.oid.off) & (align-1);
 	 size_t slop = (current_mod == 0 ? 0 : align - current_mod);
 	 size_t needed = bytes + slop;
 	 TOID(char) result = alloc_ptr_;
 	 if (needed <= alloc_bytes_remaining_) 
	 {
		 result.oid.off = alloc_ptr_.oid.off + slop;
 	   alloc_ptr_.oid.off += needed;
 	   alloc_bytes_remaining_ -= needed;

	 } else 
	 {
    // AllocateFallback always returned aligned memory
 		result = AllocateFallback(bytes);
 	 }
  	assert((reinterpret_cast<uintptr_t>(D_RW(result)) & (align-1)) == 0);
		if(!alloc_head_)
		{
			head_addr_ = result;
			alloc_head_ = true;
		}
		return result;
	}
}

TOID(char) PmemArena::AllocateNewBlock(size_t block_bytes) {
	//
	TOID(char) result;
	TOID(char) t_result;

	POBJ_ALLOC(pop, &result, char, block_bytes + 64, NULL,NULL);
	t_result = result;
	result.oid.off += 64 - (result.oid.off & 63);
	
	if(TOID_IS_NULL(block_head_))
	{
		TOID(LinkedBlock) tmp;
		POBJ_ALLOC(pop, &tmp, LinkedBlock, sizeof(LinkedBlock), NULL, NULL);

		D_RW(tmp)->block_num_ = 0;
		D_RW(tmp)->block_addr_[D_RO(tmp)->block_num_] = t_result;
		D_RW(tmp)->block_num_++;
		D_RW(tmp)->next_ = TOID_NULL(LinkedBlock);
		block_head_ = tmp;
		block_tail_ = tmp;
	}
	else
	{
		if(D_RW(block_tail_)->block_num_ < 1024)
		{
			D_RW(block_tail_)->block_addr_[D_RO(block_tail_)->block_num_] = t_result;
			D_RW(block_tail_)->block_num_++;
		}
		else
		{
			TOID(LinkedBlock) tmp;
			POBJ_ALLOC(pop, &tmp, LinkedBlock, sizeof(LinkedBlock), NULL, NULL);

			D_RW(tmp)->block_num_ = 0;
			D_RW(tmp)->block_addr_[D_RO(tmp)->block_num_] = t_result;
			D_RW(tmp)->block_num_++;
			D_RW(tmp)->next_ = TOID_NULL(LinkedBlock);
			D_RW(block_tail_)->next_ = tmp;
			block_tail_ = tmp;
		}
	}
	
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*) + sizeof(LinkedBlock)));

  return result;
}

void PmemArena::BlockAppend(TOID(LinkedBlock) head, TOID(LinkedBlock) tail)
{
	D_RW(block_tail_)->next_ = head;
	block_tail_ = tail;
}

void PmemArena::MemoryUsageUpdate(size_t block_bytes)
{
	memory_usage_.NoBarrier_Store(
			reinterpret_cast<void*>(MemoryUsage() + block_bytes));
}


}  // namespace leveldb

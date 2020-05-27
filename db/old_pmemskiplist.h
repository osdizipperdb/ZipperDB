// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_PMEMSKIPLIST_H_
#define STORAGE_LEVELDB_DB_PMEMSKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include "port/port.h"
#include "util/pmemarena.h"
#include "util/random.h"

namespace leveldb {

class PmemArena;

template<typename Key, class Comparator>
class PSkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit PSkipList(Comparator cmp) 
		: compare_(cmp),
			max_height_(reinterpret_cast<void*>(1)),
			rnd_(0xdeadbeef) {}
	void constructor(TOID(PmemArena) pmemarena, PMEMobjpool* pool);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  //int Insert(const Key& key);
	int Insert(TOID(char));

	//int InsertDomino(const Key& key);
	int InsertDomino(TOID(char));

	//inserted for test
	void Merge(TOID(char) target, TOID(char)* prev);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

	//inserted for test
	void GetPrevAddress(const Key& key, TOID(char)* prev_node_);

	void GetPrevAddressDomino(const Key& key, TOID(char)* prev_node_);

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const PSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    //const Key& key() const;
		TOID(char) key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

		TOID(char) GetAddress();

   private:
    const PSkipList* list_;
    TOID(char) node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = config::kSkiplistMaxHeight };

	//inserted for test
	TOID(char) prev_node_data_[kMaxHeight];


  // Immutable after construction
  Comparator compare_;
  TOID(PmemArena) pmemarena_;    // Arena used for allocations of nodes
	PMEMobjpool *pop;

  TOID(char) head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list

  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  Random rnd_;

  TOID(char) NewNode(TOID(char) key_buf, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, TOID(char) n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  TOID(char) FindGreaterOrEqual(const Key& key, TOID(char)* prev) const;

	TOID(char) FindGreaterOrEqualDomino(const Key& key, TOID(char)* prev) const;


  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  TOID(char) FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  TOID(char) FindLast() const;

  // No copying allowed
  PSkipList(const PSkipList&);
  void operator=(const PSkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct PSkipList<Key,Comparator>::Node {
  explicit Node(TOID(char) k, int h) : key(k), height(h) { 
		uuid = k.oid.pool_uuid_lo;
	}

  TOID(char) key;
	int height;

	const Key* ReturnKey()	{return D_RW(key);}

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  //TOID(char) Next(int n) {
	uint64_t Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
		return next_[n];
  }
  
	void SetNext(int n, uint64_t x) {
	//void SetNext(int n, TOID(char) x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    //next_[n].Release_Store(x);
		next_[n] = x;
  }

  // No-barrier variants that can be safely used in a few locations.
  //TOID(char) NoBarrier_Next(int n) {
  uint64_t NoBarrier_Next(int n) {
		assert(n >= 0);
    return next_[n];
  }

	TOID(char) GetNextNode(int n) {
		TOID(char) ret;
		ret.oid.pool_uuid_lo = uuid;
		ret.oid.off = next_[n];

		return ret;
	}

	uint64_t Getuuid() {
		return uuid;
	}

  void NoBarrier_SetNext(int n, uint64_t x) {
	//void NoBarrier_SetNext(int n, TOID(char) x) {
    assert(n >= 0);
    next_[n] = x;
  }

	void* GetBase()
	{
		return next_;
	}

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
 	uint64_t uuid;
	uint64_t next_[1];
	//TOID(char) next_[1];
};

template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::TOID(char)
TOID(char) PSkipList<Key,Comparator>::NewNode(TOID(char) key, int height) {
	unsigned int bytes = sizeof(Node) + 8 * (height - 1);
	//unsigned int bytes = sizeof(Node) + 16 * (height - 1);

  TOID(char) mem = D_RW(pmemarena_)->AllocateAligned(bytes);

	((Node*)D_RW(mem))->key = key;
	((Node*)D_RW(mem))->height = height;
	pmemobj_persist(pop, D_RW(mem), bytes);
	return mem;
  //return new (D_RW(mem)) Node(key, height);
}

template<typename Key, class Comparator>
inline PSkipList<Key,Comparator>::Iterator::Iterator(const PSkipList* list) {
  list_ = list;
  node_ = TOID_NULL(char);
}

template<typename Key, class Comparator>
inline bool PSkipList<Key,Comparator>::Iterator::Valid() const {
  return !TOID_IS_NULL(node_);
}

template<typename Key, class Comparator>
inline TOID(char) PSkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
	return ((Node*)(D_RO(node_)))->key;
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
	node_ = ((Node*)D_RW(node_))->GetNextNode(0);
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(D_RW(((Node*)D_RW(node_))->key));
  if (TOID_EQUALS(node_,list_->head_)) {
   	node_ = TOID_NULL(char);
  }
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
	//if(TOID_IS_NULL(node_)
	//		return nullptr;
	//const char* tmp = D_RO(((Node*)(D_RO(node_)))->key);
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = ((Node*)D_RO(list_->head_))->GetNextNode(0);
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (TOID_EQUALS(node_, list_->head_)) {
    node_ = TOID_NULL(char);
  }
}

//inserted for test
template<typename Key, class Comparator>
inline TOID(char) PSkipList<Key,Comparator>::Iterator::GetAddress()
{
	return node_;
}

template<typename Key, class Comparator>
int PSkipList<Key,Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template<typename Key, class Comparator>
bool PSkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, TOID(char) n) const {
	
	//bool r1 = !TOID_IS_NULL(n);
	/*bool r1 = (n.oid.off != 0);

	if(r1)
	{
		//if(TOID_IS_NULL(((Node*)D_RW(n))->key))
		//	return true;	

		Key nkey = D_RO(((Node*)D_RW(n))->key);

		bool r2 = (compare_(nkey, key) < 0);

		return (r1 && r2);
	}
	else
		return r1;*/

  return (n.oid.off != 0) && (compare_(n->key, key) < 0);
}

//inserted for test
template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::
TOID(char) PSkipList<Key,Comparator>::FindGreaterOrEqualDomino(
		const Key& key, TOID(char)* prev)
	const 
{

	int level;
	TOID(char) x = TOID_NULL(char);

	x = prev_node_data_[GetMaxHeight() - 1];
	level = GetMaxHeight() -1;

	assert(!TOID_IS_NULL(x));

	while(true)
	{
		TOID(char) next = ((Node*)D_RW(x))->GetNextNode(level);
		if (KeyIsAfterNode(key, next))
		{
			x = next;
		}
		else
		{
			prev[level] = x;
			if (level == 0)
			{
				return next;
			}
			else
			{
				level--;
				//좀 더 해볼 필요가 있음
				//if (x == nullptr)
				//	continue;
				//else if(prev_node_data_[level] == nullptr)
				//	x = prev_node_data_[level];
				//if(x != nullptr && prev_node_data_[level] != nullptr)
				//{
				//	Node* tmp = prev_node_data_[level];
				//	const Key* prevkey = prev_node_data_[level]->ReturnKey();
				//	const Key* xkey = x->ReturnKey();
				//	if( (compare_(*xkey, *prevkey) < 0) )
				//		x = prev_node_data_[level];
				//}
				//x = prev_node_data_[level];
			}
		}	
	}
}

template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::Node* 
TOID(char) PSkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, TOID(char)* prev)
    const {
  TOID(char) x = head_;
	TOID(char) next = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    next.oid.off = ((Node*)D_RO(x))->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::Node*
TOID(char)
PSkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  TOID(char) x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(TOID_EQUALS(x, head_) || compare_(D_RO(((Node*)D_RW(x))->key), key) < 0);
    TOID(char) next = ((Node*)D_RW(x))->GetNextNode(level);
    if (TOID_IS_NULL(next) || compare_(D_RO(((Node*)D_RW(next))->key), key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::Node*
TOID(char) PSkipList<Key,Comparator>::FindLast()
    const {
  TOID(char) x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    TOID(char) next = ((Node*)D_RW(x))->GetNextNode(level);
    if (TOID_IS_NULL(next)) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

//template<typename Key, class Comparator>
//PSkipList<Key,Comparator>::PSkipList() { }

template<typename Key, class Comparator>
void PSkipList<Key,Comparator>::constructor(TOID(PmemArena) pmemarena, PMEMobjpool* pool)
{
	//:	compare_(cmp),
	//	pmemarena_(pmemarena),
	//	head_(NewNode(0,kMaxHeight)),
	//	max_height_(reinterpret_cast<void*>(1)),
	//	rnd_(0xdeadbeef) {
	pmemarena_ = TOID_NULL(PmemArena);
	pmemarena_ = pmemarena;
	pop = pool;
	
	if(!D_RW(pmemarena_)->GetAllocHead())
	{
		TOID(char) tmp = TOID_NULL(char);
		head_ = NewNode(tmp,kMaxHeight);
		for (int i = 0; i < kMaxHeight; i++)
		{
			uint64_t tmp2 = 0;
			//TOID(char) tmp2 = TOID_NULL(char);
			((Node*)D_RW(head_))->SetNext(i, tmp2);
		}
		for (int i = 0; i < kMaxHeight; i++)
		{
			//uint64_t tmp2 = 0;
			TOID(char) tmp2 = TOID_NULL(char);
			prev_node_data_[i] = tmp2;
		}
	}
	else
	{
		head_ = (D_RW(pmemarena_)->GetHeadAddr()); 
		max_height_.NoBarrier_Store(reinterpret_cast<void*>(D_RW(pmemarena_)->max_height_));
	}
}

//inserted for test
template<typename Key, class Comparator>
int PSkipList<Key,Comparator>::InsertDomino(TOID(char) key)
		//const Key& key)
{
	//inserted keys are sorted already
	//therefore only need to search the key from prev_node_data_
	
	int return_value = 0;

	//////////////////////////////////////////////////////////
	bool tf = false;

	TOID(char) prev[kMaxHeight];
	TOID(char) x = FindGreaterOrEqualDomino(D_RO(key), prev);

	if(!TOID_IS_NULL(x) && Equal(D_RO(key), D_RO(((Node*)D_RW(x))->key)))
	{
		for(int i = 0; i < GetMaxHeight(); i++)
			prev_node_data_[i] = prev[i];
		return return_value;
	}

	assert(TOID_IS_NULL(x) || !Equal(D_RO(key), D_RO(((Node*)D_RW(x))->key)));

	int height = RandomHeight();
	if (height > GetMaxHeight())
	{
		for (int i = GetMaxHeight(); i < height ; i++)
		{
			prev[i] = head_;
		}
		////////////////////////////////////////////////////////////////////
		tf = true;
		max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
		D_RW(pmemarena_)->max_height_ = height;
	}

	//never happen because of previous insert
	assert(!TOID_EQUALS(prev[0], head_));
	
	//inserted key is the largest key
	if (TOID_IS_NULL(x))
		return_value = 2;

	x = NewNode(key, height);

	for (int i = 0; i < height; i++)
	{
		((Node*)D_RW(x))->NoBarrier_SetNext(i, (((Node*)D_RW(prev[i]))->NoBarrier_Next(i)));
		if(i==0)
		{
			pmemobj_persist(pop, ((Node*)D_RW(x))->GetBase(), 8);
			//pmemobj_persist(pop, ((Node*)D_RW(x))->GetBase(), 16);
		}
		
		((Node*)D_RW(prev[i]))->SetNext(i, x.oid.off);
		//((Node*)D_RW(prev[i]))->SetNext(i, x);
		
		if(i==0)
		{
			pmemobj_persist(pop, ((Node*)D_RW(prev[i]))->GetBase(), 8);
			//pmemobj_persist(pop, ((Node*)D_RW(prev[i]))->GetBase(), 16);
		}
		prev_node_data_[i] = x;
	}
	for( int i = height; i < GetMaxHeight(); i++)
	{
		//if (prev[i] != nullptr)
			prev_node_data_[i] = prev[i];
	}

	return return_value;
}

template<typename Key, class Comparator>
void PSkipList<Key,Comparator>::Merge(TOID(char) target, TOID(char)* prev)
{
	TOID(char) target_ = target;
	TOID(char)* prev_ = prev;

	int min_height = ((Node*)D_RW(target_))->height;
	//if(((Node*)D_RW(prev_))->height < min_height)
	//	min_height = ((Node*)D_RW(prev_))->height;

	for(int i = 0; i < min_height; i++)
	{
		((Node*)D_RW(target_))->NoBarrier_SetNext(i, ((Node*)D_RW(prev_[i]))->NoBarrier_Next(i));
		if(i == 0)
			pmemobj_persist(pop, ((Node*)D_RW(target_))->GetBase(), 8);
		((Node*)D_RW(prev_[i]))->NoBarrier_SetNext(i, target_.oid.off);
		if(i == 0)
			pmemobj_persist(pop, ((Node*)D_RW(prev_[i]))->GetBase(),8);
	}

}

template<typename Key, class Comparator>
int PSkipList<Key,Comparator>::Insert(TOID(char) key) {
	//	const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  //inserted for test

	//printf("Normal Insert is called\n");

	int return_value = 0;
	
	TOID(char) prev[kMaxHeight];
  TOID(char) x = FindGreaterOrEqual(D_RO(key), prev);

  // Our data structure does not allow duplicate insertion

	//inserted for test
	if(!TOID_IS_NULL(x) && Equal(D_RO(key), D_RO(((Node*)D_RW(x))->key)))
	{
		/*printf("\nsomething is wrong\n");
		printf("key: %s, x->key: %s\n",key,x->key);
		assert(false);
		exit(0);*/

		for(int i = 0; i < GetMaxHeight(); i++)
			prev_node_data_[i] = prev[i];
		return return_value;
	}
  assert(TOID_IS_NULL(x) || !Equal(D_RO(key), D_RO(((Node*)D_RW(x))->key)));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
		D_RW(pmemarena_)->max_height_ = height;
  }

	if (TOID_EQUALS(prev[0],head_))
		return_value = 1;
	else if( TOID_IS_NULL(x))
		return_value = 2;


  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    ((Node*)D_RW(x))->NoBarrier_SetNext(i, ((Node*)D_RW(prev[i]))->NoBarrier_Next(i));
    if(i == 0)
			pmemobj_persist(pop, ((Node*)D_RW(x))->GetBase(), 8);
		((Node*)D_RW(prev[i]))->SetNext(i, x.oid.off);
		if(i == 0)
			pmemobj_persist(pop, ((Node*)D_RW(prev[i]))->GetBase(), 8);
		prev_node_data_[i] = x;
  }
	for(int i = height; i < GetMaxHeight(); i++)
	{
		if (!TOID_IS_NULL(prev[i]))
			prev_node_data_[i] = prev[i];
	}

	return return_value;
}

template<typename Key, class Comparator>
void PSkipList<Key,Comparator>::GetPrevAddress(const Key& key, TOID(char)* prev_node_)
{
	TOID(char) prev[kMaxHeight];
	TOID(char) x = FindGreaterOrEqual(key, prev);

	assert(TOID_IS_NULL(x) || !Equal(key, D_RO(((Node*)D_RW(x))->key)));

	for(int i = 0; i < GetMaxHeight(); i++)
	{
		if (!TOID_IS_NULL(prev[i]))
		{
			prev_node_data_[i] = prev[i];
			prev_node_[i] = prev[i];
		}
	}
	for(int i = GetMaxHeight(); i < kMaxHeight; i++)
		prev_node_[i] = head_;

	return ;
}

template<typename Key, class Comparator>
void PSkipList<Key,Comparator>::GetPrevAddressDomino(const Key& key, TOID(char)* prev_node_)
{
	TOID(char) prev[kMaxHeight];
	TOID(char) x = FindGreaterOrEqualDomino(key, prev);

	assert(TOID_IS_NULL(x) || !Equal(key, D_RO(((Node*)D_RW(x))->key)));

	for(int i = 0; i < GetMaxHeight(); i++)
	{
		if (!TOID_IS_NULL(prev[i]))
		{
			prev_node_data_[i] = prev[i];
			prev_node_[i] = prev[i];
		}
	}
	for(int i = GetMaxHeight(); i < kMaxHeight; i++)
		prev_node_[i] = head_;

	return ;
}

template<typename Key, class Comparator>
bool PSkipList<Key,Comparator>::Contains(const Key& key) const {
  TOID(char) tmp = TOID_NULL(char);
	TOID(char) x = FindGreaterOrEqual(key, tmp);
  if (!TOID_IS_NULL(x) && Equal(key, D_RO(((Node*)D_RW(x))->key))) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PMEMSKIPLIST_H_

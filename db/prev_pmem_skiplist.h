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
#include "port/port.h"
#include "util/pmemarena.h"
#include "util/random.h"

namespace leveldb {

class PmemArena;

template<typename Key, class Comparator>
class PSkipList {

 public:
  struct Node;
	// Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit PSkipList(Comparator cmp) 
		: compare_(cmp),
			max_height_(reinterpret_cast<void*>(1)),
			rnd_(0xdeadbeef) {}
	void constructor(TOID(PmemArena) pmemarena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  int Insert(const Key& key);

	int InsertDomino(const Key& key);

	//inserted for test
	void Merge(void* target, void* prev);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

	//inserted for test
	void* GetPrevAddress(const Key& key);

	void* GetPrevAddressDomino(const Key& key);

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
    const Key& key() const;

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

		void* GetAddress();

   private:
    const PSkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 20 };

	//inserted for test
	Node* prev_node_data_[kMaxHeight];


  // Immutable after construction
  Comparator compare_;
  TOID(PmemArena) pmemarena_;    // Arena used for allocations of nodes

  Node* head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list

  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  Random rnd_;

  TOID(Node) NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, TOID(Node) n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  TOID(Node) FindGreaterOrEqual(const Key& key, TOID(Node)* prev) const;

	TOID(Node) FindGreaterOrEqualDomino(const Key& key, TOID(Node)* prev) const;


  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  TOID(Node) FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  TOID(Node) FindLast() const;

  // No copying allowed
  PSkipList(const PSkipList&);
  void operator=(const PSkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct PSkipList<Key,Comparator>::Node {
  explicit Node(const Key& k, int h) : key(k) { }

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  TOID(Node) Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    //return reinterpret_cast<Node*>(next_[n].Acquire_Load());
		return next_[n];
  }
  void SetNext(int n, TOID(Node) x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    //next_[n].Release_Store(x);
		next_[n] = x;
  }

  // No-barrier variants that can be safely used in a few locations.
  TOID(Node) NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n];
  }
  void NoBarrier_SetNext(int n, TOID(Node) x) {
    assert(n >= 0);
    next_[n] = x;
  }

	TOID(Node) GetBase()
	{
		return next_[0];
	}

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  TOID(Node) next_[1];
};

template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::TOID(Node)
TOID(Node)
PSkipList<Key,Comparator>::NewNode(const Key& key, int height) {
	unsigned int bytes = sizeof(Node) + 16 * (height - 1);
  TOID(Node) mem;
	mem = (TOID(Node))(D_RW(pmemarena_)->AllocateAligned(bytes));

	return new (mem) Node(key);
	//Node* tmp = new Node(key);
	//memcpy(D_RW(mem), tmp, bytes);
  //return new (mem) Node(key, height);
	
	//memcpy(D_RW(mem), tmp, sizeof(tmp));

	//return (Node*) D_RW(mem);
	
	

	return mem;

	//return (Node*) D_RW(mem);
}

template<typename Key, class Comparator>
inline PSkipList<Key,Comparator>::Iterator::Iterator(const PSkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template<typename Key, class Comparator>
inline bool PSkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template<typename Key, class Comparator>
inline const Key& PSkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator>
inline void PSkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

//inserted for test
template<typename Key, class Comparator>
inline void* PSkipList<Key,Comparator>::Iterator::GetAddress()
{
	return (void*)node_;
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
bool PSkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, TOID(Node) n) const {
  // null n is considered infinite
	//
	
	//inserted for test
	Node* n_tmp = D_RW(n);
	bool r1 = (n_tmp != nullptr);

	if(r1)
	{
		Key nkey = n_tmp->key;

		bool r2 = (compare_(nkey, key) < 0);

		return (r1 && r2);
	}
	else
		return r1;

  //return (n != nullptr) && (compare_(n->key, key) < 0);
}

//inserted for test
template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::
TOID(Node) PSkipList<Key,Comparator>::FindGreaterOrEqualDomino(
		const Key& key, TOID(Node)* prev)
	const 
{

	int level;
	TOID(Node) x = TOID_NULL(Node);

	/*
	for (int i = GetMaxHeight() -1 ; i >= 0; i--)
	{
		if (prev_node_data_[i] == head_)
		{
			if(prev != nullptr)
				prev[i] = head_;
			continue;
		}
		else
		{
			x = prev_node_data_[i];
			level = i;
			break;
		}
	}*/

	x = prev_node_data_[GetMaxHeight() - 1];
	level = GetMaxHeight() -1;

	assert(D_RO(x) != nullptr);

	while(true)
	{
		TOID(Node) next = D_RW(x)->Next(level);
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
				/*if (x == nullptr)
					continue;
				else if(prev_node_data_[level] == nullptr)
					x = prev_node_data_[level];
				else
				{
					Key prevkey = prev_node_data_[level]->key;
					Key xkey = x->key;
					if( (compare_(xkey, prevkey) < 0) )
					x = prev_node_data_[level];
				}*/
				x = prev_node_data_[level];
			}
		}	
	}
}

template<typename Key, class Comparator>
//typename PSkipList<Key,Comparator>::
TOID(Node) PSkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, TOID(Node)* prev)
    const {
  TOID(Node) x = head_;
	TOID(Node) tmp = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    TOID(Node) next = D_RW(x)->Next(level);
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
//typename PSkipList<Key,Comparator>::
TOID(Node)
PSkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  TOID(Node) x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(D_RO(x)->key, key) < 0);
    TOID(Node) next = D_RW(x)->Next(level);
    if (next == nullptr || compare_(D_RO(next)->key, key) >= 0) {
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
//typename PSkipList<Key,Comparator>::
TOID(Node) PSkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    TOID(Node) next = D_RW(x)->Next(level);
    if (next == nullptr) {
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
void PSkipList<Key,Comparator>::constructor(TOID(PmemArena) pmemarena)
{
	//:	compare_(cmp),
	//	pmemarena_(pmemarena),
	//	head_(NewNode(0,kMaxHeight)),
	//	max_height_(reinterpret_cast<void*>(1)),
	//	rnd_(0xdeadbeef) {
	TOID(PmemArena) tmp;
	pmemarena_ = TOID_NULL(PmemArena);
	pmemarena_ = pmemarena;
	tmp = pmemarena_;
	
	if(!D_RW(pmemarena_)->GetAllocHead())
	{
		//printf("head does not exist, allocate new\n");
		head_ = NewNode(0,kMaxHeight);
		for (int i = 0; i < kMaxHeight; i++)
			head_->SetNext(i, nullptr);
		for (int i = 0; i < kMaxHeight; i++)
			prev_node_data_[i] = nullptr;
	}
	else
	{
		//printf("head already exist, get addr\n");
		head_ = (D_RW(pmemarena_)->GetHeadAddr()); 
		max_height_.NoBarrier_Store(reinterpret_cast<void*>(D_RW(pmemarena_)->max_height_));
	}
}

//inserted for test
template<typename Key, class Comparator>
int PSkipList<Key,Comparator>::InsertDomino(const Key& key)
{
	//inserted keys are sorted already
	//therefore only need to search the key from prev_node_data_
	
	int return_value = 0;

	//////////////////////////////////////////////////////////
	bool tf = false;

	TOID(Node) prev[kMaxHeight];
	TOID(Node) x = FindGreaterOrEqualDomino(key, prev);

	if(D_RO(x) != nullptr && Equal(key, D_RO(x)->key))
	{
		/*printf("\nsomething is wrong\n");
		printf("key: %s, x->key: %s\n",key,x->key);
		assert(false);
		exit(0);*/

		for(int i = 0; i < GetMaxHeight(); i++)
			prev_node_data_[i] = prev[i];
		return return_value;
	}

	assert(x == nullptr || !Equal(key, D_RO(x)->key));

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
	assert(prev[0] != head_);
	
	//inserted key is the largest key
	if (D_RO(x) == nullptr)
		return_value = 2;

	x = NewNode(key, height);

/*	
	//inserted for test
	printf("\n\n");
	printf("head is : %lu\n", (unsigned long int)((void*)head_));
	printf("Inserted node is : %lu\n", (unsigned long int)((void*)x));
	printf("Inserted height is : %d\n", height);
	int lv = GetMaxHeight() - 1;
	if(tf)
		lv--;
	for(int i = lv; i >= 0; i--)
	{
		printf("level: %d, node: %lu\n",i, (unsigned long int)((void*)prev_node_data_[i]));
	}
	printf("\n\n");
	/////////////////////////////////////////////////////////////////////
	*/


	for (int i = 0; i < height; i++)
	{
		D_RW(x)->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
		prev[i]->SetNext(i, x);
		prev_node_data_[i] = x;
	}
	for( int i = height; i < GetMaxHeight(); i++)
	{
		if (prev[i] != nullptr)
			prev_node_data_[i] = prev[i];
	}

	/*
	//inserted for test
	
	printf("\n");
	for (int i = GetMaxHeight() - 1; i >= 0; i--)
	{
		printf("level: %d, prev_node: %lu -> cur_node: %lu\n", i, (unsigned long int)((void*)prev[i]), (unsigned long int)((void*)prev[i]->Next(i)));
	}
	printf("\n\n");
	/////////////////////////////////////////////////////////////////////////
*/

	return return_value;
}

template<typename Key, class Comparator>
void PSkipList<Key,Comparator>::Merge(TOID(char) target, TOID(char)* prev)
{
	TOID(char) target_ = target;
	TOID(char)* prev_ = prev;

	int min_height = target_->height;
	if(prev_->height < min_height)
		min_height = prev_->height;

	for(int i = 0; i < min_height; i++)
	{
		target_->NoBarrier_SetNext(i, prev_->NoBarrier_Next(i));
		prev_->NoBarrier_SetNext(i, target_);
		if(i == 0)
		{
			pmemobj_persist(D_RW(pmemarena_)->GetPool(), D_RW(target_->GetBase()), 16);
			pmemobj_persist(D_RW(pmemarena_)->GetPool(), D_RW(prev_->GetBase()), 16);
		}
	}

}

template<typename Key, class Comparator>
int PSkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  //inserted for test

	//printf("Normal Insert is called\n");

	int return_value = 0;
	
	TOID(Node) prev[kMaxHeight];
  TOID(Node) x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion

	//inserted for test
	if(x != nullptr && Equal(key, D_RO(x)->key))
	{
		/*printf("\nsomething is wrong\n");
		printf("key: %s, x->key: %s\n",key,x->key);
		assert(false);
		exit(0);*/

		for(int i = 0; i < GetMaxHeight(); i++)
			prev_node_data_[i] = prev[i];
		return return_value;
	}
  assert(x == nullptr || !Equal(key, D_RO(x)->key));

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

	if (prev[0] == head_)
		return_value = 1;
	else if( x == nullptr)
		return_value = 2;


  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    D_RW(x)->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
		prev_node_data_[i] = x;
  }
	for(int i = height; i < GetMaxHeight(); i++)
	{
		if (prev[i] != nullptr)
			prev_node_data_[i] = prev[i];
	}

	return return_value;
}

template<typename Key, class Comparator>
void* PSkipList<Key,Comparator>::GetPrevAddress(const Key& key)
{
	TOID(Node) prev[kMaxHeight];
	TOID(Node) x = FindGreaterOrEqual(key, prev);

	assert(x == nullptr || !Equal(key, D_RO(x)->key));

	for(int i = 0; i < GetMaxHeight(); i++)
	{
		if (prev[i] != nullptr)
			prev_node_data_[i] = prev[i];
	}

	return (void*)(prev[0]);
}

template<typename Key, class Comparator>
void* PSkipList<Key,Comparator>::GetPrevAddressDomino(const Key& key)
{
	TOID(Node) prev[kMaxHeight];
	TOID(Node) x = FindGreaterOrEqualDomino(key, prev);

	assert(x == nullptr || !Equal(key, D_RO(x)->key));

	for(int i = 0; i < GetMaxHeight(); i++)
	{
		if (prev[i] != nullptr)
			prev_node_data_[i] = prev[i];
	}

	return (void*)(prev[0]);
}

template<typename Key, class Comparator>
bool PSkipList<Key,Comparator>::Contains(const Key& key) const {
  TOID(Node) x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, D_RO(x)->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PMEMSKIPLIST_H_

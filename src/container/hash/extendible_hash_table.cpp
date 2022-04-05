//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  Page *p = buffer_pool_manager_->NewPage(&directory_page_id_);
  if (p == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "bpm is full");
  }

  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(p->GetData());
  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id);
  dir_page->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  bool res = bucket_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  if (!bucket_page->IsFull()) {
    bool res = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(bucket_page_id, res);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return res;
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  bool res = SplitInsert(transaction, key, value);
  table_latch_.WUnlock();
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t old_page_id = dir_page->GetBucketPageId(dir_index);
  HASH_TABLE_BUCKET_TYPE *old_page = FetchBucketPage(old_page_id);

  page_id_t new_page_id;
  Page *p = buffer_pool_manager_->NewPage(&new_page_id);
  if (p == nullptr) {
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return false;
  }
  HASH_TABLE_BUCKET_TYPE *new_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(p->GetData());

  uint8_t old_depth = dir_page->GetLocalDepth(dir_index);
  dir_page->IncrLocalDepth(dir_index);

  // 需要增加global depth
  if (dir_page->GetLocalDepth(dir_index) > dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
    uint32_t new_index_beg = 1 << (dir_page->GetGlobalDepth() - 1);
    for (uint32_t i = new_index_beg; i < 2 * new_index_beg; i++) {
      dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(i - new_index_beg));
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(i - new_index_beg));
    }
  }

  // 原本指向oldpage的指针，根据其第(old_depth+1)位是否为1决定是否将其指向newpage
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (0 == ((i ^ dir_index) & ((1 << old_depth) - 1))) {
      dir_page->SetLocalDepth(i, old_depth + 1);
      if (1 == ((i >> old_depth) & 1)) {
        dir_page->SetBucketPageId(i, new_page_id);
      } else {
        dir_page->SetBucketPageId(i, old_page_id);
      }
    }
  }

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (old_page->IsReadable(i)) {
      KeyType k = old_page->KeyAt(i);
      ValueType v = old_page->ValueAt(i);

      if (KeyToPageId(k, dir_page) == static_cast<uint32_t>(new_page_id)) {
        old_page->Remove(k, v, comparator_);
        new_page->Insert(k, v, comparator_);
      }
    }
  }

  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(old_page_id, true);

  // 插入新元素
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  if (!bucket_page->IsFull()) {
    bool res = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    return res;
  }

  // 如果local depth+1还是不足以分开这些元素，需要再加一位继续分，直到能分开为止。
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  return SplitInsert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  bool res = bucket_page->Remove(key, value, comparator_);

  if (bucket_page->IsEmpty()) {
    uint32_t buddy_index = dir_page->GetSplitImageIndex(bucket_index);
    uint32_t old_depth = dir_page->GetLocalDepth(bucket_index);

    if (old_depth != 0 && old_depth == dir_page->GetLocalDepth(buddy_index)) {
      Merge(transaction, key, value);
      buffer_pool_manager_->FlushPage(bucket_page_id);
      buffer_pool_manager_->UnpinPage(bucket_page_id, false);
      buffer_pool_manager_->DeletePage(bucket_page_id);
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);
      table_latch_.WUnlock();
      return res;
    }
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, res);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.WUnlock();
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);

  uint32_t buddy_index = dir_page->GetSplitImageIndex(bucket_index);
  page_id_t buddy_page_id = dir_page->GetBucketPageId(buddy_index);

  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      dir_page->SetBucketPageId(i, buddy_page_id);
      dir_page->DecrLocalDepth(i);
    } else if (dir_page->GetBucketPageId(i) == buddy_page_id) {
      dir_page->DecrLocalDepth(i);
    }
  }

  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

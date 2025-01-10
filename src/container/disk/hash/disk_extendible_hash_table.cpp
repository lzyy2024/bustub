//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  BasicPageGuard header_page = bpm_->NewPageGuarded(&header_page_id_);
  WritePageGuard header_page_w = header_page.UpgradeWrite();
  auto ht_header = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page_w.GetDataMut());
  ht_header->Init(header_max_depth);
  header_page_w.Drop();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 Transaction *transaction) const -> bool {
  ReadPageGuard header_page_r = bpm_->FetchPageRead(header_page_id_);
  auto header = reinterpret_cast<const ExtendibleHTableHeaderPage *>(header_page_r.GetData());
  uint32_t hash = Hash(key);
  uint32_t directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_page_r.Drop();

  ReadPageGuard directory_page_r = bpm_->FetchPageRead(directory_page_id);
  auto directory = reinterpret_cast<const ExtendibleHTableDirectoryPage *>(directory_page_r.GetData());
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_page_r.Drop();

  ReadPageGuard bucket_page_r = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page_r.GetData());
  V value;
  if (bucket->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  WritePageGuard header_page_w = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_page_w.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }
  header_page_w.Drop();

  WritePageGuard directory_page_w = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_page_w.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }
  directory_page_w.Drop();

  WritePageGuard bucket_page_w = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket->IsFull()) {
    return bucket->Insert(key, value, cmp_);
  }
  while (bucket->IsFull()) {
    uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
    uint32_t global_depth = directory->GetGlobalDepth();
    if (global_depth == local_depth && global_depth == directory_max_depth_) {  // full
      return false;
    }
    // new bucket
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    BasicPageGuard new_bucket_page = bpm_->NewPageGuarded(&new_bucket_page_id);
    if (new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    WritePageGuard new_bucket_page_w = new_bucket_page.UpgradeWrite();
    auto new_bucket = new_bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);
    if (global_depth == local_depth) {
      directory->IncrGlobalDepth();
      global_depth = directory->GetGlobalDepth();
    }
    auto split_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    MigrateEntries(bucket, new_bucket, split_bucket_idx, 1 << local_depth);
    directory->IncrLocalDepth(bucket_idx);
    local_depth = directory->GetLocalDepth(bucket_idx);
    directory->SetBucketPageId(split_bucket_idx, new_bucket_page_id);
    directory->SetLocalDepth(split_bucket_idx, local_depth);
    auto count = 1 << (global_depth - local_depth);
    auto mask = directory->GetLocalDepthMask(split_bucket_idx);
    auto stride = 1 << local_depth;
    for (auto i = 0; i < count; i++) {
      directory->SetBucketPageId((split_bucket_idx & mask) + i * stride, new_bucket_page_id);
      directory->SetLocalDepth((split_bucket_idx & mask) + i * stride, local_depth);
    }
    if (directory->HashToBucketIndex(hash) != bucket_idx) {
      bucket = new_bucket;
      bucket_idx = split_bucket_idx;
    }
  }
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id = INVALID_PAGE_ID;
  BasicPageGuard directory_page = bpm_->NewPageGuarded(&directory_page_id);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  WritePageGuard directory_page_w = directory_page.UpgradeWrite();
  auto directory = directory_page_w.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  return InsertToNewBucket(directory, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard bucket_page = bpm_->NewPageGuarded(&bucket_page_id);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_page_w = bucket_page.UpgradeWrite();
  auto bucket = bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
  auto count = 1 << (directory->GetGlobalDepth());
  auto stride = 1;
  uint32_t mask = directory->GetLocalDepthMask(bucket_idx);
  for (int i = 0; i < count; i++) {
    directory->SetLocalDepth((bucket_idx & mask) + i * stride, 0);
    directory->SetBucketPageId((bucket_idx & mask) + i * stride, bucket_page_id);
  }
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (int i = static_cast<int>(old_bucket->Size()) - 1; i >= 0; i--) {
    K key = old_bucket->KeyAt(i);
    uint32_t hash = Hash(key);
    if ((hash & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      auto &entry = old_bucket->EntryAt(i);
      new_bucket->Insert(entry.first, entry.second, cmp_);
      old_bucket->RemoveAt(i);
    }
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // std::cout << "remove: " << key << std::endl;
  uint32_t hash = Hash(key);
  WritePageGuard header_page_w = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_page_w.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t dirctory_index = header->HashToDirectoryIndex(hash);
  page_id_t dirctory_page_id = header->GetDirectoryPageId(dirctory_index);
  // dirctory_page_id not found.
  if (dirctory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_page_w.Drop();
  WritePageGuard dirctory_page_w = bpm_->FetchPageWrite(dirctory_page_id);
  auto directory = dirctory_page_w.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_page_w = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_w = bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool is_successful = bucket_w->Remove(key, cmp_);
  if (!is_successful) {
    // remove fail
    return false;
  }
  bucket_page_w.Drop();

  // up is release
  ReadPageGuard bucket_page_r = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = bucket_page_r.As<ExtendibleHTableBucketPage<K, V, KC>>();
  uint32_t local_depth = directory->GetLocalDepth(bucket_index);
  uint32_t merge_mask;
  while (local_depth > 0) {
    merge_mask = 1 << (local_depth - 1);
    uint32_t global_depth = directory->GetGlobalDepth();
    uint32_t merge_bucket_index = merge_mask ^ bucket_index;
    uint32_t merge_local_depth = directory->GetLocalDepth(merge_bucket_index);
    ReadPageGuard merge_bucket_page = bpm_->FetchPageRead(directory->GetBucketPageId(merge_bucket_index));
    auto merge_bucket = merge_bucket_page.As<ExtendibleHTableBucketPage<K, V, KC>>();
    if (local_depth > 0 && local_depth == merge_local_depth && (merge_bucket->IsEmpty() || bucket->IsEmpty())) {
      if (merge_bucket->IsEmpty()) {
        bpm_->DeletePage(directory->GetBucketPageId(merge_bucket_index));
        directory->DecrLocalDepth(bucket_index);
        directory->DecrLocalDepth(merge_bucket_index);
        directory->SetBucketPageId(merge_bucket_index, bucket_page_id);
        auto count = 1 << (global_depth - local_depth + 1);
        u_int32_t mask = directory->GetLocalDepthMask(bucket_index);
        auto stride = 1 << (local_depth - 1);
        for (auto i = 0; i < count; i++) {
          directory->SetLocalDepth((bucket_index & mask) + i * stride, directory->GetLocalDepth(bucket_index));
          directory->SetBucketPageId((bucket_index & mask) + i * stride, bucket_page_id);
        }

      } else {
        bpm_->DeletePage(bucket_page_id);
        directory->DecrLocalDepth(merge_bucket_index);
        directory->DecrLocalDepth(bucket_index);
        directory->SetBucketPageId(bucket_index, directory->GetBucketPageId(merge_bucket_index));
        auto count = 1 << (global_depth - merge_local_depth + 1);
        u_int32_t mask = directory->GetLocalDepthMask(merge_bucket_index);
        auto stride = 1 << (merge_local_depth - 1);
        for (auto i = 0; i < count; i++) {
          directory->SetLocalDepth((merge_bucket_index & mask) + i * stride,
                                   directory->GetLocalDepth(merge_bucket_index));
          directory->SetBucketPageId((merge_bucket_index & mask) + i * stride,
                                     directory->GetBucketPageId(merge_bucket_index));
        }
        bucket_index = merge_bucket_index;
        bucket = merge_bucket;
        bucket_page_id = directory->GetBucketPageId(merge_bucket_index);
      }
      local_depth = directory->GetLocalDepth(bucket_index);
    } else {
      break;
    }
  }
  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

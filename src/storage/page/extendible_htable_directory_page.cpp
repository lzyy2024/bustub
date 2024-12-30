//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (int i = 0; i < (1 << max_depth_); i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  uint32_t mask = (1 << global_depth_) - 1;
  return hash & mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= (1U << max_depth_)) {
    return;
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1 << local_depths_[bucket_idx]);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << local_depths_[bucket_idx]) - 1;
}
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ < max_depth_) {
    uint32_t upper = (1 << (global_depth_ + 1));
    for (uint32_t i = (1 << global_depth_); i != upper; i++) {
      bucket_page_ids_[i] = bucket_page_ids_[HashToBucketIndex(i)];
      local_depths_[i] = local_depths_[HashToBucketIndex(i)];
    }
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  global_depth_--;
  uint32_t upper = (1 << (global_depth_ + 1));
  for (uint32_t i = (1 << global_depth_); i != upper; i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
    local_depths_[i] = 0;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < (1U << global_depth_); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (1 << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx < (1U << max_depth_)) {
    local_depths_[bucket_idx] = local_depth;
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  uint32_t local_depth = local_depths_[bucket_idx];
  local_depths_[bucket_idx]++;
  auto count = 1 << (global_depth_ - local_depth);
  uint32_t mask = (1 << local_depth) - 1;
  auto stride = 1 << local_depth;
  for (auto i = 0; i != count; i++) {
    uint32_t next_bucket_idx = (bucket_idx & mask) + i * stride;
    bucket_page_ids_[next_bucket_idx] = bucket_page_ids_[bucket_idx];
    local_depths_[next_bucket_idx] = local_depth + 1;
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub

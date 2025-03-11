//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // std::cout << "LRUKReplacer:" << num_frames << ' ' << k << '\n';
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // std::cout << "lru_replacer.Evict( ";
  std::lock_guard<std::mutex> lock(latch_);

  if (!ppriority_evict_.empty()) {
    auto [time, id] = *ppriority_evict_.begin();
    ppriority_evict_.erase(ppriority_evict_.begin());
    *frame_id = id;
    auto &node = node_store_[id];
    node.Erase();

    // std::cout << *frame_id << '\n';
    return true;
  }
  if (!priority_evict_.empty()) {
    auto [distance, id] = *priority_evict_.begin();
    priority_evict_.erase(priority_evict_.begin());
    *frame_id = id;
    auto &node = node_store_[id];
    node.Erase();
    // std::cout << *frame_id << '\n';
    return true;
  }

  return false;
}

void LRUKReplacer::DeleteLast(size_t frame_id) {
  // delete last
  auto node = node_store_[frame_id];
  size_t last = node.GetLast();
  size_t count = node.GetCnt();
  if (last != 0) {
    if (count < k_) {
      auto it = ppriority_evict_.find(last);
      ppriority_evict_.erase(it);
    } else {
      auto it = priority_evict_.find(last);
      priority_evict_.erase(it);
    }
  }
};

void LRUKReplacer::AddNewOne(size_t frame_id) {
  auto node = node_store_[frame_id];
  size_t last = node.GetLast();
  size_t count = node.GetCnt();
  if (last != 0) {
    if (count < k_) {
      ppriority_evict_[last] = frame_id;
    } else {
      priority_evict_[last] = frame_id;
    }
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  // std::cout << "lru_replacer.RecordAccess( " << frame_id << " );\n";

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("LRUKReplacer::RecordAccess: frame_id is invalid");
  }

  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.count(frame_id) == 0) {
    LRUKNode node(k_, frame_id);
    node_store_.emplace(frame_id, node);
  }

  if (access_type != AccessType::Scan) {
    auto &node = node_store_[frame_id];
    auto flag = node.CheckEvictable();
    if (flag) {
      DeleteLast(frame_id);
    }
    // add new one
    node.RecordAccess(++current_timestamp_);
    node.SetLast(node.GetDistance());
    node.SetCnt(node.GetCnt() + 1);
    if (flag) {
      AddNewOne(frame_id);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // std::cout << "lru_replacer.SetEvictable(" << frame_id << ' ' << set_evictable << " );\n";
  std::lock_guard<std::mutex> guard(latch_);
  if (0U == node_store_.count(frame_id)) {
    return;
  }

  auto &node = node_store_[frame_id];
  if (node.CheckEvictable() == set_evictable) {
    return;
  }

  if (!set_evictable) {
    DeleteLast(frame_id);
  } else {
    AddNewOne(frame_id);
  }
  node.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // std::cout << "Remove" << ' ' << frame_id << '\n';
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.count(frame_id) == 0U) {
    return;
  }
  auto node = node_store_[frame_id];
  if (!node.CheckEvictable()) {
    throw Exception("LRUKReplacer::Remove: frame_id can not be remove");
  }
  DeleteLast(frame_id);
  node.Erase();
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  curr_size_ = ppriority_evict_.size() + priority_evict_.size();
  // std::cout << "size():" << ppriority_evict_.size() << ' ' << priority_evict_.size() << '\n';
  // printf("lru_replacer.Size(): %ld\n", curr_size_);
  return curr_size_;
}

}  // namespace bustub

auto bustub::LRUKNode::RecordAccess(size_t current_timestamp_) -> size_t {
  history_.push_back(current_timestamp_);
  if (history_.size() > k_) {
    history_.pop_front();
  }
  return GetDistance();
}

auto bustub::LRUKNode::GetDistance() -> size_t { return history_.front(); }

void bustub::LRUKNode::SetEvictable(bool set_evictable_) { is_evictable_ = set_evictable_; }

void bustub::LRUKNode::Erase() {
  cnt_ = 0;
  last_ = 0;
  history_.clear();
  SetEvictable(false);
}

auto bustub::LRUKNode::GetCnt() -> size_t { return cnt_; }
auto bustub::LRUKNode::GetLast() -> size_t { return last_; }
void bustub::LRUKNode::SetLast(size_t last) { last_ = last; }
void bustub::LRUKNode::SetCnt(size_t cnt) { cnt_ = cnt; }
auto bustub::LRUKNode::CheckEvictable() -> bool { return is_evictable_; }
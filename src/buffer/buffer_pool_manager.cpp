//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // std::cout << "NewPage:\n";
  Page *page;
  frame_id_t frame_id = -1;
  std::scoped_lock lock(latch_);
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  page = pages_ + frame_id;
  if (page->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  *page_id = AllocatePage();
  // std::cout << *page_id << '\n';
  page_table_.erase(page->GetPageId());
  page_table_.emplace(*page_id, frame_id);
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->ResetMemory();

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // std::cout << "FetchPage" << ' ' << page_id << '\n';
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  std::scoped_lock lock(latch_);

  if (page_table_.count(page_id) != 0U) {
    auto frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    auto page = pages_ + frame_id;
    page->pin_count_++;
    return page;
  }

  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  auto page = pages_ + frame_id;
  if (page->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  page_table_.erase(page->GetPageId());
  page_table_.emplace(page_id, frame_id);
  page->ResetMemory();
  page->pin_count_ = 1;
  page->page_id_ = page_id;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // std::cout << "UnpinPage" << ' ' << page_id << '\n';
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) != 0U) {
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    if (is_dirty) {
      page->is_dirty_ = is_dirty;
    }

    if (page->GetPinCount() == 0) {
      return false;
    }
    page->pin_count_ -= 1;
    if (page->GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }

  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // std::cout << "FlushPage" << page_id << '\n';
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto page = pages_ + page_table_[page_id];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();

  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // std::cout << "FlushAllPages\n";
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto &[page_id, frame_id] : page_table_) {
    auto page = pages_ + frame_id;

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // std::cout << "Delete" << ' ' << page_id << '\n';
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) != 0U) {
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    if (page->GetPinCount() > 0) {
      return false;
    }

    page_table_.erase(page_id);
    free_list_.push_back(frame_id);
    replacer_->Remove(frame_id);

    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  }

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub

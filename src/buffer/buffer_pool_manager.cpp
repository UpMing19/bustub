//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// $ make buffer_pool_manager_test -j$(nproc)
// $ ./test/buffer_pool_manager_test
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

#define debug(x) std::cout << #x << " : " << (x) << std::endl;

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
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
  std::unique_lock<std::mutex> l(latch_);

  if (!free_list_.empty()) {
    debug(free_list_.size());
    auto fr = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    page_table_[*page_id] = fr;
    pages_[fr].pin_count_++;
    pages_[fr].page_id_ = *page_id;
    replacer_->RecordAccess(fr);
    replacer_->SetEvictable(fr, false);
    return &pages_[fr];
  }
  frame_id_t fr = -1;
  bool res = replacer_->Evict(&fr);
  if (!res || fr == -1) {
    return nullptr;
  }

  debug(free_list_.size());
  debug(fr);
  debug(res);

  if (pages_[fr].page_id_ != INVALID_PAGE_ID && pages_[fr].is_dirty_) {
    InternalFlushPages(pages_[fr].page_id_);
  }
  pages_[fr].ResetMemory();
  pages_[fr].is_dirty_ = false;
  pages_[fr].pin_count_ = 0;
  page_table_.erase(pages_[fr].page_id_);

  *page_id = AllocatePage();
  page_table_[*page_id] = fr;
  pages_[fr].pin_count_++;
  pages_[fr].page_id_ = *page_id;
  replacer_->RecordAccess(fr);
  replacer_->SetEvictable(fr, false);
  return &pages_[fr];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> l(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    debug(page_id);
    frame_id_t fr;
    if (!free_list_.empty()) {
      fr = free_list_.front();
      free_list_.pop_front();
    } else {
      bool res = replacer_->Evict(&fr);
      if (!res) {
        return nullptr;
      }

      debug(free_list_.size());
      debug(fr);
      debug(res);
    }

    if (pages_[fr].is_dirty_) {
      InternalFlushPages(pages_[fr].page_id_);
    }
    page_table_.erase(pages_[fr].page_id_);
    pages_[fr].ResetMemory();
    pages_[fr].page_id_ = page_id;
    pages_[fr].pin_count_++;
    pages_[fr].is_dirty_ = false;
    page_table_[page_id] = fr;
    replacer_->RecordAccess(fr);
    replacer_->SetEvictable(fr, false);
    disk_manager_->ReadPage(page_id, pages_[fr].data_);
    return &pages_[page_table_[page_id]];
  }
  debug(page_table_[page_id]);
  replacer_->RecordAccess(page_table_[page_id]);
  replacer_->SetEvictable(page_table_[page_id], false);
  pages_[page_table_[page_id]].pin_count_++;
  return &pages_[page_table_[page_id]];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ == 0) {
      return false;
    }
    pages_[frame_id].pin_count_--;
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    pages_[frame_id].is_dirty_ = is_dirty;
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  return InternalFlushPages(page_id);
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> l(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    InternalFlushPages(pages_[i].page_id_);
  }
}
auto BufferPoolManager::InternalFlushPages(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ > 0) {
      return false;
    }
    replacer_->Remove(frame_id);
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    free_list_.push_back(frame_id);
    DeallocatePage(page_id);
    page_table_.erase(page_id);
    return true;
  }
  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *p = FetchPage(page_id);
  p->RLatch();
  return {this, p};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *p = FetchPage(page_id);
  p->WLatch();
  return {this, p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub

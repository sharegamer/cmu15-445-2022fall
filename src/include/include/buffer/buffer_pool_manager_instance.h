//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * @brief Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the lookback constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable logging). Please ignore this for P1.
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                            LogManager *log_manager = nullptr);

  /**
   * @brief Destroy an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;

  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t override { return pool_size_; }

  /** @brief Return the pointer to all the pages in the buffer pool. */
  auto GetPages() -> Page * { return pages_; }

 protected:
  /**
   * TODO(P1): Add implementation
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  auto NewPgImp(page_id_t *page_id) -> Page * override {
    std::scoped_lock<std::mutex> lock(latch_);
    frame_id_t new_frame;
    if (!GetNewframe(new_frame)) {
      return nullptr;
    }
    // std::cout<<new_frame<<"\n";
    *page_id = AllocatePage();
    pages_[new_frame].is_dirty_ = false;
    pages_[new_frame].page_id_ = *page_id;
    pages_[new_frame].pin_count_ = 1;
    replacer_->RecordAccess(new_frame);
    replacer_->SetEvictable(new_frame, false);
    page_table_->Insert(*page_id, new_frame);
    return &pages_[new_frame];
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
   * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
   * to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
   *
   * @param page_id id of page to be fetched
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   */
  auto FetchPgImp(page_id_t page_id) -> Page * override {
    std::scoped_lock<std::mutex> lock(latch_);
    frame_id_t frame_id;
    if (page_table_->Find(page_id, frame_id)) {
      replacer_->RecordAccess(frame_id);
      pages_[frame_id].pin_count_++;
      replacer_->SetEvictable(frame_id, false);
      return &pages_[frame_id];
    }
    if (GetNewframe(frame_id)) {
      page_table_->Insert(page_id, frame_id);
      disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
      pages_[frame_id].pin_count_ = 1;
      pages_[frame_id].page_id_ = page_id;
      pages_[frame_id].is_dirty_ = false;
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      return &pages_[frame_id];
    }

    return nullptr;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override {
    std::scoped_lock<std::mutex> lock(latch_);
    frame_id_t frame;
    if (page_table_->Find(page_id, frame)) {
      if (pages_[frame].pin_count_ <= 0) {
        return false;
      }
      if (!pages_[frame].is_dirty_) {
        pages_[frame].is_dirty_ = is_dirty;
      }
      pages_[frame].pin_count_--;
      if (pages_[frame].pin_count_ == 0) {
        replacer_->SetEvictable(frame, true);
      }
      return true;
    }
    return false;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPgImp(page_id_t page_id) -> bool override {
    std::scoped_lock<std::mutex> lock(latch_);
    frame_id_t frame;
    if (page_table_->Find(page_id, frame)) {
      disk_manager_->WritePage(page_id, pages_[frame].data_);
      pages_[frame].is_dirty_ = false;
      return true;
    }
    return false;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override {
    std::scoped_lock<std::mutex> lock(latch_);
    bool a[pool_size_];
    for (size_t i = 0; i < pool_size_; i++) {
      a[i] = true;
    }
    for (auto item : free_list_) {
      a[item] = false;
    }
    for (size_t i = 0; i < pool_size_; i++) {
      if (a[i]) {
        disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
        pages_[i].is_dirty_ = false;
      }
    }
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePgImp(page_id_t page_id) -> bool override {
    std::scoped_lock<std::mutex> lock(latch_);
    frame_id_t frame;
    if (page_table_->Find(page_id, frame)) {
      if (pages_[frame].pin_count_ > 0) {
        return false;
      }
      if (pages_[frame].is_dirty_) {
        disk_manager_->WritePage(page_id, pages_[frame].data_);
      }
      page_table_->Remove(page_id);
      replacer_->Remove(frame);
      free_list_.push_back(frame);
      pages_[frame].ResetMemory();
      DeallocatePage(page_id);
      return true;
    }
    return true;
  }

  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;
  /** Bucket size for the extendible hash table */
  const size_t bucket_size_ = 4;

  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. Please ignore this for P1. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  ExtendibleHashTable<page_id_t, frame_id_t> *page_table_;
  /** Replacer to find unpinned pages for replacement. */
  LRUKReplacer *replacer_;
  /** List of free frames that don't have any pages on them. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  // TODO(student): You may add additional private members and helper functions

  // get a free frame,assign it to frame_id,return true if success
  auto GetNewframe(frame_id_t &frame_id) -> bool {
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
      return true;
    }
    if (replacer_->Evict(&frame_id)) {
      if (pages_[frame_id].is_dirty_) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
        pages_[frame_id].is_dirty_ = false;
      }
      pages_[frame_id].ResetMemory();
      page_table_->Remove(pages_[frame_id].page_id_);
      return true;
    }
    return false;
  }
};
}  // namespace bustub

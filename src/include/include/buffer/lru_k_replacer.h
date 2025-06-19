//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <iterator>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);

    if (curr_size_ == 0) {
      return false;
    }
    size_t earliest = -1;
    bool find = false;
    std::list<FrameStats>::iterator curr;
    for (auto item = non_full_list_.begin(); item != non_full_list_.end(); item++) {
      if (item->IsEvitable()) {
        find = true;
        if (item->GetTime() < earliest) {
          curr = item;
          earliest = item->GetTime();
        }
      }
    }
    if (find) {
      *frame_id = curr->GetId();
      non_full_list_.erase(curr);
      curr_size_--;
      return true;
    }

    for (auto item = full_list_.begin(); item != full_list_.end(); item++) {
      if (item->IsEvitable()) {
        find = true;
        if (item->GetTime() < earliest) {
          curr = item;
          earliest = item->GetTime();
        }
      }
    }
    if (find) {
      *frame_id = curr->GetId();
      full_list_.erase(curr);
      curr_size_--;
      return true;
    }
    return find;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    if (static_cast<size_t>(frame_id) > replacer_size_) {
      throw std::invalid_argument(std::string("error"));
    }
    current_timestamp_++;

    std::list<FrameStats>::iterator curr;
    for (auto item = non_full_list_.begin(); item != non_full_list_.end(); item++) {
      if (item->GetId() == frame_id) {
        item->AddTime(current_timestamp_);
        if (item->IsFull()) {
          full_list_.push_back(*item);
          non_full_list_.erase(item);
        }
        return;
      }
    }

    for (auto &item : full_list_) {
      if (item.GetId() == frame_id) {
        item.AddTime(current_timestamp_);
        return;
      }
    }
    curr_size_++;
    FrameStats new_frame(frame_id, current_timestamp_, k_);
    if (new_frame.IsFull()) {
      full_list_.push_back(new_frame);
    } else {
      non_full_list_.push_back(new_frame);
    }
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);

    if (static_cast<size_t>(frame_id) > replacer_size_) {
      throw std::invalid_argument(std::string("error"));
    }

    for (auto &item : non_full_list_) {
      if (item.GetId() == frame_id) {
        if (item.SetEvitable(set_evictable)) {
          if (set_evictable) {
            curr_size_++;

          } else {
            curr_size_--;
          }
        }
        return;
      }
    }

    for (auto &item : full_list_) {
      if (item.GetId() == frame_id) {
        if (item.SetEvitable(set_evictable)) {
          if (set_evictable) {
            curr_size_++;

          } else {
            curr_size_--;
          }
        }
        return;
      }
    }
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    if (static_cast<size_t>(frame_id) > replacer_size_) {
      throw std::invalid_argument(std::string("error"));
    }
    for (auto item = non_full_list_.begin(); item != non_full_list_.end(); item++) {
      if (item->GetId() == frame_id) {
        if (item->IsEvitable()) {
          curr_size_--;
          non_full_list_.erase(item);
          return;
        }
        throw std::invalid_argument(std::string("error"));
      }
    }
    for (auto item = full_list_.begin(); item != full_list_.end(); item++) {
      if (item->GetId() == frame_id) {
        if (item->IsEvitable()) {
          curr_size_--;
          full_list_.erase(item);
          return;
        }

        throw std::invalid_argument(std::string("error"));
      }
    }
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t { return curr_size_; }

  class FrameStats {
   public:
    size_t num_;  // num of timestamp
    std::list<size_t> time_;
    bool evitable_;
    size_t k_;
    frame_id_t id_;
    FrameStats(frame_id_t frame_id, size_t t, size_t k) {
      time_.push_back(t);
      num_ = 1;
      evitable_ = true;
      k_ = k;
      id_ = frame_id;
    }

    // set evitable,return true if changes
    auto SetEvitable(bool e) -> bool {
      bool change = e != evitable_;
      evitable_ = e;
      return change;
    }
    // return number of timestamp
    auto Number() -> size_t { return num_; }

    auto IsFull() -> bool { return num_ == k_; }

    auto IsEvitable() -> bool { return evitable_; }

    void AddTime(size_t add_time) {
      if (num_ == k_) {
        time_.pop_front();
        time_.push_back(add_time);
      } else {
        time_.push_back(add_time);
        num_++;
      }
    }

    auto GetId() -> frame_id_t { return id_; }

    auto GetTime() -> size_t { return *(time_.begin()); }
  };
  std::list<FrameStats> full_list_;
  std::list<FrameStats> non_full_list_;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub

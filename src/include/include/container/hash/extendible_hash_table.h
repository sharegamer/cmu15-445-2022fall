//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <iostream>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override {
    std::scoped_lock<std::mutex> lock(latch_);

    int index = IndexOf(key);
    return dir_[index]->Find(key, value);
  }

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override {
    std::scoped_lock<std::mutex> lock(latch_);
    int index = IndexOf(key);
    std::shared_ptr<Bucket> bucket = dir_[index];
    V mid;
    while (!(bucket->Find(key, mid) || !bucket->IsFull())) {
      int depth = bucket->GetDepth();
      if (depth == global_depth_) {
        for (int i = 0; i < (1 << global_depth_); i++) {
          dir_.push_back(dir_[i]);
        }
        global_depth_ += 1;
      }
      auto pair_list = dir_[index]->GetItems();
      std::shared_ptr<Bucket> new1_bucket(new Bucket(bucket_size_, depth + 1));
      std::shared_ptr<Bucket> new2_bucket(new Bucket(bucket_size_, depth + 1));
      num_buckets_ += 1;
      int begin = (1 << depth) + InnerIndex(key, depth);
      int step = (1 << (depth + 1));
      for (; begin < (1 << global_depth_); begin += step) {
        dir_[begin] = new2_bucket;
      }
      begin = InnerIndex(key, depth);
      for (; begin < (1 << global_depth_); begin += step) {
        dir_[begin] = new1_bucket;
      }
      std::list<std::pair<K, V>> new_list;
      auto now = pair_list.begin();
      while (now != pair_list.end()) {
        new_list.push_back(*now);
        now++;
      }
      for (auto &item : new_list) {
        int item_index = IndexOf(item.first);
        dir_[item_index]->Insert(item.first, item.second);
      }
      index = IndexOf(key);
      bucket = dir_[index];
    }

    bucket->Insert(key, value);
  }

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override {
    std::scoped_lock<std::mutex> lock(latch_);

    int index = IndexOf(key);
    return dir_[index]->Remove(key);
  }

  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool {
      bool ret = false;
      for (auto &key_pair : list_) {
        if (key == key_pair.first) {
          ret = true;
          value = key_pair.second;
          break;
        }
      }
      return ret;
    }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool {
      bool ret = false;
      auto now = list_.begin();
      while (now != list_.end()) {
        if (now->first == key) {
          ret = true;
          now = list_.erase(now);
        } else {
          now++;
        }
      }
      return ret;
    }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
    auto Insert(const K &key, const V &value) -> bool {
      bool find = false;
      for (auto &key_pair : list_) {
        if (key_pair.first == key) {
          key_pair.second = value;
          find = true;
        }
      }
      if (IsFull() && !find) {
        return false;
      }
      if (!find) {
        list_.push_back(std::pair<K, V>(key, value));
      }
      return true;
    }

   private:
    // TODO(student): You may add additional private members and helper functions
    size_t size_;
    int depth_;
    std::list<std::pair<K, V>> list_;
  };

  void Debug() {
    for (size_t i = 0; i < dir_.size(); i++) {
      std::cout << "num:" << i << "  depth:" << dir_[i]->GetDepth() << "  ";
      for (std::pair<K, V> &item : dir_[i]->GetItems()) {
        std::cout << "key:" << item.first << " ";
      }
      std::cout << '\n';
    }
  }

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table

  auto InnerIndex(const K &key, int depth) -> size_t {
    int mask = (1 << depth) - 1;
    return std::hash<K>()(key) & mask;
  }

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub

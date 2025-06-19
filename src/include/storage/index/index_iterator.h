//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index_/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t id_, int index_, BufferPoolManager *bmp_);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return id_ == itr.id_ && index_ == itr.index_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return id_ != itr.id_ || index_ != itr.index_; }

 private:
  // add your own private member variables here
  page_id_t id_;
  int index_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  BufferPoolManager *bmp_;
  MappingType pair_;
};

}  // namespace bustub

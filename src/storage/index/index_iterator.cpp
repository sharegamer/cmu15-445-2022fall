/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t id, int index, BufferPoolManager *bmp) : id_(id), index_(index), bmp_(bmp) {
  if (id_ != INVALID_PAGE_ID) {
    leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bmp_->FetchPage(id_)->GetData());
  } else {
    leaf_page_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (leaf_page_ != nullptr && leaf_page_->GetNextPageId() == INVALID_PAGE_ID &&
          leaf_page_->GetSize() - 1 == index_);
}

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (leaf_page_ != nullptr) {
    pair_ = leaf_page_->PairAt(index_);
  } else {
    pair_ = MappingType();
  }
  return pair_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (id_ == INVALID_PAGE_ID) {
    return *this;
  }
  if (index_ < leaf_page_->GetSize() - 1) {
    index_++;

  } else {
    index_ = 0;
    page_id_t pre = id_;
    if (leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
      id_ = leaf_page_->GetNextPageId();
      leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bmp_->FetchPage(id_)->GetData());
    } else {
      id_ = INVALID_PAGE_ID;
      leaf_page_ = nullptr;
    }
    bmp_->UnpinPage(pre, false);
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

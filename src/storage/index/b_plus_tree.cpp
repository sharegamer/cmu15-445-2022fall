#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // leaf_max_size_ = 2;
  // internal_max_size_ = 3;
  // std::cout << "leaf_max_size_:" << leaf_max_size_ << '\n';
  // std::cout << "internal_max_size_:" << internal_max_size_ << '\n';
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // std::shared_lock<std::shared_mutex> locker(shared_mutex_);
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return false;
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = GetLeafPage(key, nullptr, OpType::Find, true);
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      buffer_pool_manager_->FetchPage(leaf_page->GetPageId())->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return true;
    }
  }
  buffer_pool_manager_->FetchPage(leaf_page->GetPageId())->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return false;
}

// concurrent edition
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, Transaction *transaction, OpType op, bool first_pass)
    -> B_PLUS_TREE_LEAF_PAGE_TYPE * {
  if (!first_pass) {
    root_latch_.WLock();
    transaction->AddIntoPageSet(nullptr);
  }
  page_id_t next_page_id = root_page_id_;
  Page *prev_page = nullptr;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (first_pass) {
      if (tree_page->IsLeafPage() && op != OpType::Find) {
        page->WLatch();
        transaction->AddIntoPageSet(page);
      } else {
        page->RLatch();
      }
      if (prev_page != nullptr) {
        prev_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
      } else {
        root_latch_.RUnlock();
      }
    } else {
      page->WLatch();
      if (IsPageSafe(tree_page, op)) {
        ReleaseWLatches(transaction);
      }
      transaction->AddIntoPageSet(page);
    }

    if (tree_page->IsLeafPage()) {
      if (first_pass && !IsPageSafe(tree_page, op)) {
        ReleaseWLatches(transaction);
        return GetLeafPage(key, transaction, op, false);
      }
      return reinterpret_cast<LeafPage *>(tree_page);
    }
    auto internal_page = static_cast<InternalPage *>(tree_page);
    next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    for (int i = 1; i < internal_page->GetSize(); i++) {
      if (comparator_(internal_page->KeyAt(i), key) > 0) {
        next_page_id = internal_page->ValueAt(i - 1);
        break;
      }
    }
    prev_page = page;
  }
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) -> B_PLUS_TREE_LEAF_PAGE_TYPE * {
//   auto *cur = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

//   while (!cur->IsLeafPage()) {
//     page_id_t next_page = cur->ValueAt(cur->GetSize() - 1);
//     for (int i = 1; i < cur->GetSize(); i++) {
//       if (comparator_(key, cur->KeyAt(i)) < 0) {
//         next_page = cur->ValueAt(i - 1);
//         break;
//       }
//     }
//     buffer_pool_manager_->UnpinPage(cur->GetPageId(), false);
//     cur = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page)->GetData());
//   }
//   auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(cur);
//   return leaf;
// }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // std::cout << "insert:key:" << key << "  vaule:" << value << "\n";
  // std::unique_lock<std::shared_mutex> locker(shared_mutex_);
  // situation1 tree is empty
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    root_latch_.WLock();
    if (IsEmpty()) {
      Page *root = buffer_pool_manager_->NewPage(&root_page_id_);
      auto *root_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(root->GetData());
      root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      root_page->SetKeyAt(0, key);
      root_page->SetValueAt(0, value);
      root_page->SetSize(1);
      UpdateRootPageId(1);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      root_latch_.WUnlock();
      return true;
    }
    root_latch_.WUnlock();
    root_latch_.RLock();
  }

  // check duplicate
  B_PLUS_TREE_LEAF_PAGE_TYPE *cur = GetLeafPage(key, transaction, OpType::Insert, true);
  for (int i = 0; i < cur->GetSize(); i++) {
    if (comparator_(key, cur->KeyAt(i)) == 0) {
      ReleaseWLatches(transaction);
      return false;
    }
  }

  // insert pair to leaf
  cur->Insert(std::make_pair(key, value), comparator_);
  // return if not full
  if (cur->GetSize() < leaf_max_size_) {
    ReleaseWLatches(transaction);
    return true;
  }

  // leaf page full
  page_id_t leaf_id;
  Page *new_leaf = buffer_pool_manager_->NewPage(&leaf_id);
  auto *new_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_leaf->GetData());
  new_page->Init(leaf_id, cur->GetParentPageId(), leaf_max_size_);
  new_page->SetNextPageId(cur->GetNextPageId());
  cur->SetNextPageId(leaf_id);
  for (int i = 0, start = (leaf_max_size_ + 1) / 2; start < leaf_max_size_; i++, start++) {
    new_page->SetPairAt(i, cur->PairAt(start));
  }
  cur->SetSize((leaf_max_size_ + 1) / 2);
  new_page->SetSize(leaf_max_size_ - (leaf_max_size_ + 1) / 2);

  // if cur is root page
  if (cur->GetPageId() == root_page_id_) {
    Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    UpdateRootPageId(0);
    auto *root = reinterpret_cast<InternalPage *>(root_page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->SetValueAt(0, cur->GetPageId());
    root->SetPairAt(1, std::make_pair(new_page->KeyAt(0), leaf_id));
    root->SetSize(2);
    cur->SetParentPageId(root_page_id_);
    new_page->SetParentPageId(root_page_id_);
    ReleaseWLatches(transaction);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }

  // if cur is not root page

  InsertInternal(cur->GetParentPageId(), std::make_pair(new_page->KeyAt(0), leaf_id), cur->GetPageId());
  ReleaseWLatches(transaction);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  return true;
}

// insert pair into internal node
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInternal(page_id_t parent, std::pair<KeyType, page_id_t> data, page_id_t value) {
  Page *parent_page = buffer_pool_manager_->FetchPage(parent);
  auto *parentpage = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // get index of pre child node
  int index = parentpage->Indexbyvalue(value);
  // if parentpage not full
  if (parentpage->GetSize() < parentpage->GetMaxSize()) {
    for (int i = parentpage->GetSize(); i > index + 1; i--) {
      parentpage->SetPairAt(i, parentpage->PairAt(i - 1));
    }

    parentpage->SetPairAt(index + 1, data);
    parentpage->IncreaseSize(1);
    buffer_pool_manager_->UnpinPage(parent, true);
    return;
  }
  // if parent is full
  page_id_t internal_id;
  Page *new_internal = buffer_pool_manager_->NewPage(&internal_id);
  auto *new_page = reinterpret_cast<InternalPage *>(new_internal->GetData());
  new_page->Init(internal_id, parentpage->GetParentPageId(), internal_max_size_);

  // if added key in left
  if ((index + 1) < ((internal_max_size_ + 2) / 2)) {
    for (int i = 0, j = ((internal_max_size_ + 2) / 2 - 1); j < internal_max_size_; j++, i++) {
      new_page->SetPairAt(i, parentpage->PairAt(j));
      auto *child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parentpage->ValueAt(j))->GetData());
      child->SetParentPageId(internal_id);
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }
    for (int i = ((internal_max_size_ + 2) / 2 - 1); i > index + 1; i--) {
      parentpage->SetPairAt(i, parentpage->PairAt(i - 1));
    }
    parentpage->SetPairAt(index + 1, data);
  } else {
    // if added key in right
    for (int i = 0, j = ((internal_max_size_ + 2) / 2); j < internal_max_size_; i++, j++) {
      new_page->SetPairAt(i, parentpage->PairAt(j));
      auto *child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parentpage->ValueAt(j))->GetData());
      child->SetParentPageId(internal_id);
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }
    new_page->SetSize(internal_max_size_ - (internal_max_size_ + 2) / 2);

    for (int i = new_page->GetSize(); i > (index + 1 - (internal_max_size_ + 2) / 2); i--) {
      new_page->SetPairAt(i, new_page->PairAt(i - 1));
    }
    new_page->SetPairAt(index + 1 - (internal_max_size_ + 2) / 2, data);
    auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(data.second)->GetData());
    child->SetParentPageId(internal_id);
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }

  parentpage->SetSize((internal_max_size_ + 2) / 2);
  new_page->SetSize(internal_max_size_ + 1 - (internal_max_size_ + 2) / 2);

  // if parent is root page
  if (parent == root_page_id_) {
    Page *new_root = buffer_pool_manager_->NewPage(&root_page_id_);
    auto *root_page = reinterpret_cast<InternalPage *>(new_root->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    UpdateRootPageId(0);
    root_page->SetValueAt(0, parent);
    root_page->SetPairAt(1, std::make_pair(new_page->KeyAt(0), internal_id));
    root_page->IncreaseSize(2);
    parentpage->SetParentPageId(root_page_id_);
    new_page->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(parentpage->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  // if parent if not root
  InsertInternal(parentpage->GetParentPageId(), std::make_pair(new_page->KeyAt(0), internal_id),
                 parentpage->GetPageId());
  buffer_pool_manager_->UnpinPage(parentpage->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::cout << "remove:key:" << key << "\n";

  // std::unique_lock<std::shared_mutex> locker(shared_mutex_);
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return;
  }

  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = GetLeafPage(key, transaction, OpType::Delete, true);
  leaf_page->Remove(key, comparator_);
  // if leaf is root
  if (leaf_page->GetPageId() == root_page_id_) {
    // if leaf size=0
    if (leaf_page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
      header_page->DeleteRecord(index_name_);
      buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
      ReleaseWLatches(transaction);
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
      return;
    }
    ReleaseWLatches(transaction);
    return;
  }

  // if leaf is not underflow
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    ReleaseWLatches(transaction);
    return;
  }

  // make balance of num pin and unpin
  buffer_pool_manager_->FetchPage(leaf_page->GetPageId());

  HandleLeafUnderflow(leaf_page, transaction);
  ReleaseWLatches(transaction);
  auto deleted_pages = transaction->GetDeletedPageSet();
  for (auto &pid : *deleted_pages) {
    buffer_pool_manager_->DeletePage(pid);
  }
}

// deal with leaf underflow
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleLeafUnderflow(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, Transaction *transaction) {
  int leaf_index;
  int bro_index;
  auto *parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId())->GetData());
  auto *bro = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(GetBrotherPage(parent, leaf_page, leaf_index, bro_index));
  buffer_pool_manager_->FetchPage(bro->GetPageId())->WLatch();
  buffer_pool_manager_->UnpinPage(bro->GetPageId(), true);
  // node size enough
  if (bro->GetSize() > bro->GetMinSize()) {
    if (leaf_index > bro_index) {  // left node
      leaf_page->Insert(bro->PairAt(bro->GetSize() - 1), comparator_);
      bro->IncreaseSize(-1);
      parent->SetKeyAt(leaf_index, leaf_page->KeyAt(0));
    } else {
      // right node
      leaf_page->Insert(bro->PairAt(0), comparator_);
      bro->Remove(std::move(bro->KeyAt(0)), comparator_);
      parent->SetKeyAt(bro_index, bro->KeyAt(0));
    }
    buffer_pool_manager_->FetchPage(bro->GetPageId())->WUnlatch();
    buffer_pool_manager_->UnpinPage(bro->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(bro->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return;
  }

  // merge node
  B_PLUS_TREE_LEAF_PAGE_TYPE *src_page;
  B_PLUS_TREE_LEAF_PAGE_TYPE *des_page;
  int src_index;
  if (bro_index < leaf_index) {
    des_page = bro;
    src_page = leaf_page;
    src_index = leaf_index;
  } else {
    des_page = leaf_page;
    src_page = bro;
    src_index = bro_index;
  }
  des_page->InsertPage(src_page);
  parent->Remove(src_index);
  des_page->SetNextPageId(src_page->GetNextPageId());

  transaction->AddIntoDeletedPageSet(src_page->GetPageId());

  if (parent->GetSize() < parent->GetMinSize()) {
    if (!(root_page_id_ == parent->GetPageId())) {
      /* not root page underflow */
      HandleInternalUnderflow(parent, transaction);
    } else if (parent->GetSize() == 1) {
      /* parent is root and only des_page child*/
      root_page_id_ = des_page->GetPageId();
      des_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(false);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
  } else {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  buffer_pool_manager_->FetchPage(bro->GetPageId())->WUnlatch();
  buffer_pool_manager_->UnpinPage(bro->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(src_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(des_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleInternalUnderflow(InternalPage *target_page, Transaction *transaction) {
  // fetch from pool get bro
  int tar_index;
  int bro_index;
  auto parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(target_page->GetParentPageId())->GetData());
  auto bro_page = static_cast<InternalPage *>(GetBrotherPage(parent_page, target_page, tar_index, bro_index));
  buffer_pool_manager_->FetchPage(bro_page->GetPageId())->WLatch();
  buffer_pool_manager_->UnpinPage(bro_page->GetPageId(), true);
  if (bro_page->GetSize() > bro_page->GetMinSize()) {
    if (bro_index < tar_index) {
      KeyType bro_last_key = bro_page->KeyAt(bro_page->GetSize() - 1);
      page_id_t bro_last_value = bro_page->ValueAt(bro_page->GetSize() - 1);
      bro_page->Remove(bro_page->GetSize() - 1);
      target_page->SetKeyAt(0, parent_page->KeyAt(tar_index));
      target_page->InsertByIndex(0, bro_last_key, bro_last_value);
      auto child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(target_page->ValueAt(0))->GetData());
      child->SetParentPageId(target_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      parent_page->SetKeyAt(tar_index, bro_last_key);
    } else {
      KeyType bro_first_key = parent_page->KeyAt(bro_index);
      page_id_t bro_first_value = bro_page->ValueAt(0);
      bro_page->Remove(0);
      target_page->InsertByIndex(target_page->GetSize(), bro_first_key, bro_first_value);
      auto child = reinterpret_cast<BPlusTreePage *>(
          buffer_pool_manager_->FetchPage(target_page->ValueAt(target_page->GetSize() - 1))->GetData());
      child->SetParentPageId(target_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      parent_page->SetKeyAt(bro_index, bro_page->KeyAt(0));
    }
    buffer_pool_manager_->FetchPage(bro_page->GetPageId())->WUnlatch();
    buffer_pool_manager_->UnpinPage(bro_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(bro_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(target_page->GetPageId(), true);
    return;
  }

  InternalPage *src_page;
  InternalPage *des_page;
  int src_index;

  if (bro_index < tar_index) {
    /* left_bro <- target */
    src_page = target_page;
    des_page = bro_page;
    src_index = tar_index;
  } else {
    /* target <- right_bro */
    src_page = bro_page;
    des_page = target_page;
    src_index = bro_index;
  }

  src_page->SetKeyAt(0, FindFistKey(src_page));  // insert key
  src_page->MoveAllDataTo(des_page, comparator_, buffer_pool_manager_);
  parent_page->Remove(src_index);

  transaction->AddIntoDeletedPageSet(src_page->GetPageId());

  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    if (!(parent_page->GetPageId() == root_page_id_)) {
      HandleInternalUnderflow(parent_page, transaction);
    } else if (parent_page->GetSize() == 1) {
      root_page_id_ = des_page->GetPageId();
      des_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(false);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(parent_page->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
  } else {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  buffer_pool_manager_->FetchPage(bro_page->GetPageId())->WUnlatch();
  buffer_pool_manager_->UnpinPage(bro_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(src_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(des_page->GetPageId(), true);
}

// return bro index and page
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBrotherPage(InternalPage *parent_page, BPlusTreePage *child_page, int &target_index,
                                    int &bro_index) -> BPlusTreePage * {
  page_id_t child_id = child_page->GetPageId();
  target_index = parent_page->Indexbyvalue(child_id);

  // only right child
  if (target_index == 0) {
    bro_index = 1;
    return reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(1)));
  }
  // only left child
  if (target_index == (parent_page->GetSize() - 1)) {
    bro_index = target_index - 1;
    return reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(bro_index)));
  }

  // have two child
  auto lbro =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(target_index - 1)));
  auto rbro =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(target_index + 1)));

  if (rbro->GetSize() > rbro->GetMinSize() && lbro->GetSize() <= lbro->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(lbro->GetPageId(), false);
    bro_index = target_index + 1;
    return rbro;
  }
  buffer_pool_manager_->UnpinPage(rbro->GetPageId(), false);
  bro_index = target_index - 1;
  return lbro;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindFistKey(InternalPage *target_page) -> KeyType {
  assert(root_page_id_ != INVALID_PAGE_ID);

  auto cur_page =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(target_page->ValueAt(0))->GetData());

  while (!cur_page->IsLeafPage()) {
    auto internal_page = static_cast<InternalPage *>(cur_page);
    cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(0))->GetData());
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  }
  auto key = static_cast<LeafPage *>(cur_page)->KeyAt(0);
  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *page, OpType op) -> bool {
  if (op == OpType::Find) {
    return true;
  }
  if (op == OpType::Insert) {
    if (((page->IsLeafPage() && page->GetSize() < page->GetMaxSize() - 1)) ||
        (!page->IsLeafPage() && page->GetSize() < page->GetMaxSize())) {
      return true;
    }
  }
  if (op == OpType::Delete) {
    // root
    if (page->GetPageId() == root_page_id_) {
      if (page->IsLeafPage()) {
        return page->GetSize() > 1;
      }
      return page->GetSize() > 2;
    }
    return page->GetSize() > page->GetMinSize();
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseWLatches(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    page_set->pop_front();
    if (page == nullptr) {
      root_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_);
  }
  auto *cur = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!cur->IsLeafPage()) {
    page_id_t pre = cur->GetPageId();
    cur = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(cur->ValueAt(0))->GetData());
    buffer_pool_manager_->UnpinPage(pre, false);
  }
  page_id_t id = cur->GetPageId();
  buffer_pool_manager_->UnpinPage(id, false);
  return INDEXITERATOR_TYPE(id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  LeafPage *leafpage = GetLeafPage(key, nullptr, OpType::Find, true);
  int index = 0;
  for (; index < leafpage->GetSize(); index++) {
    if (comparator_(key, leafpage->KeyAt(index)) <= 0) {
      break;
    }
  }
  page_id_t id = leafpage->GetPageId();
  buffer_pool_manager_->UnpinPage(id, false);
  return INDEXITERATOR_TYPE(id, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

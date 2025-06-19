//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_oid_t index_id = plan_->GetIndexOid();
  IndexInfo *index = exec_ctx_->GetCatalog()->GetIndex(index_id);
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(index->table_name_)->table_.get();
  auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index->index_.get());
  ite_ = tree->GetBeginIterator();
  end_ = tree->GetEndIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ite_ == end_) {
    return false;
  }
  *rid = (*ite_).second;
  table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++ite_;
  return true;
}

}  // namespace bustub

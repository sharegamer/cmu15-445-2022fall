//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (insert_) {
    return false;
  }
  insert_ = true;
  Tuple child_tuple{};
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int insert_count = 0;
  // Get the next tuple
  while (child_executor_->Next(&child_tuple, rid)) {
    bool suc = table_info->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    if (!suc) {
      return false;
    }

    for (const auto index : indexes) {
      Tuple key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                 index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    insert_count++;
  }
  std::vector<Value> result{Value(INTEGER, insert_count)};
  *tuple = Tuple(result, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub

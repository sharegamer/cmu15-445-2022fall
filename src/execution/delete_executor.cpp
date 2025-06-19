//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (delete_) {
    return false;
  }
  delete_ = true;
  Tuple child_tuple{};
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int delete_count = 0;
  // Get the next tuple
  while (child_executor_->Next(&child_tuple, rid)) {
    bool suc = table_info->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (!suc) {
      return false;
    }

    for (const auto index : indexes) {
      Tuple key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                 index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    delete_count++;
  }
  std::vector<Value> result{Value(INTEGER, delete_count)};
  *tuple = Tuple(result, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub

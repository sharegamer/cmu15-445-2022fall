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

  // Acquire intention exclusive lock on the table
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  auto table_oid = plan_->TableOid();

  try {
    // Check if we already have a sufficient lock on the table
    if (!txn->IsTableExclusiveLocked(table_oid) && !txn->IsTableIntentionExclusiveLocked(table_oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(table_oid)) {
      // Acquire IX lock on the table for row-level locking
      if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid)) {
        throw ExecutionException("Failed to acquire IX lock on table for insert");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Transaction aborted during table lock acquisition for insert");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (insert_) {
    return false;
  }
  insert_ = true;

  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();

  Tuple child_tuple{};
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int insert_count = 0;

  // Get the next tuple
  while (child_executor_->Next(&child_tuple, rid)) {
    // Insert the tuple into the table
    bool suc = table_info->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    if (!suc) {
      return false;
    }

    // After successful insertion, acquire exclusive lock on the new row
    try {
      // For inserts, we need exclusive lock on the new row
      if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *rid)) {
        // If we can't get the lock, we need to rollback the insert
        // In a real system, we'd mark this tuple as aborted
        throw ExecutionException("Failed to acquire exclusive lock on inserted row");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Transaction aborted during row lock acquisition for insert");
    }

    // Update all indexes
    for (const auto index : indexes) {
      Tuple key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                 index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }

    insert_count++;
  }

  // Return the number of inserted tuples
  std::vector<Value> result{Value(INTEGER, insert_count)};
  *tuple = Tuple(result, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub

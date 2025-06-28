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

void DeleteExecutor::Init() {
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
        throw ExecutionException("Failed to acquire IX lock on table for delete");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Transaction aborted during table lock acquisition for delete");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (delete_) {
    return false;
  }
  delete_ = true;

  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();

  Tuple child_tuple{};
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int delete_count = 0;

  // Get the next tuple
  while (child_executor_->Next(&child_tuple, rid)) {
    try {
      // Check if we already have an exclusive lock on this row
      if (!txn->IsRowExclusiveLocked(plan_->TableOid(), *rid)) {
        // If we only have a shared lock (from child executor), upgrade to exclusive
        if (txn->IsRowSharedLocked(plan_->TableOid(), *rid)) {
          // The lock manager will handle the upgrade internally
          if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *rid)) {
            throw ExecutionException("Failed to upgrade to exclusive lock on row for delete");
          }
        } else {
          // No lock at all, acquire exclusive lock
          if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *rid)) {
            throw ExecutionException("Failed to acquire exclusive lock on row for delete");
          }
        }
      }
      // If we already have exclusive lock, proceed with delete
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Transaction aborted during row lock acquisition for delete");
    }
    // Mark the tuple as deleted
    bool suc = table_info->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (!suc) {
      throw ExecutionException("Failed to mark tuple as deleted");
    }

    // Update all indexes
    for (const auto index : indexes) {
      Tuple key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                 index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }

    delete_count++;
  }

  // Return the number of deleted tuples
  std::vector<Value> result{Value(INTEGER, delete_count)};
  *tuple = Tuple(result, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub

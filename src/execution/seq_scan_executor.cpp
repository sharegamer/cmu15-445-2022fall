//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      table_iterator_(table_info_->table_->Begin(exec_ctx->GetTransaction())),
      end_iterator_(table_info_->table_->End()) {}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  
  // READ_UNCOMMITTED doesn't need any locks for reads
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return;
  }
  
  // For READ_COMMITTED and REPEATABLE_READ, acquire IS lock on table
  try {
    // Check if we already have a sufficient lock
    if (!txn->IsTableExclusiveLocked(table_info_->oid_) && 
        !txn->IsTableSharedLocked(table_info_->oid_) &&
        !txn->IsTableIntentionExclusiveLocked(table_info_->oid_) &&
        !txn->IsTableSharedIntentionExclusiveLocked(table_info_->oid_) &&
        !txn->IsTableIntentionSharedLocked(table_info_->oid_)) {
      // Acquire IS lock on the table
      if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_info_->oid_)) {
        throw ExecutionException("Failed to acquire IS lock on table");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Transaction aborted during lock acquisition");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_ == end_iterator_) {
    return false;
  }
  
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  
  *tuple = *table_iterator_;
  *rid = tuple->GetRid();
  
  // Handle row locking based on isolation level
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      // Check if we already have a lock on this row
      if (!txn->IsRowExclusiveLocked(table_info_->oid_, *rid) &&
          !txn->IsRowSharedLocked(table_info_->oid_, *rid)) {
        // Acquire shared lock on the row
        if (!lock_manager->LockRow(txn, LockManager::LockMode::SHARED, table_info_->oid_, *rid)) {
          throw ExecutionException("Failed to acquire shared lock on row");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Transaction aborted during row lock acquisition");
    }
  }
  
  table_iterator_++;
  return true;
}

}  // namespace bustub
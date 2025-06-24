//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // part1
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  // part2
  LockMode txn_lockmode;
  bool already_lock = false;
  if (txn->IsTableExclusiveLocked(oid)) {
    already_lock = true;
    txn_lockmode = LockMode::EXCLUSIVE;
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    already_lock = true;
    txn_lockmode = LockMode::INTENTION_EXCLUSIVE;
  } else if (txn->IsTableIntentionSharedLocked(oid)) {
    already_lock = true;
    txn_lockmode = LockMode::INTENTION_SHARED;
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    already_lock = true;
    txn_lockmode = LockMode::SHARED_INTENTION_EXCLUSIVE;
  } else if (txn->IsTableSharedLocked(oid)) {
    already_lock = true;
    txn_lockmode = LockMode::SHARED;
  }

  // part3
  if (already_lock) {
    if (txn_lockmode == lock_mode) {
      return true;
    } else if (txn_lockmode == LockMode::SHARED &&
               (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);

    } else if (txn_lockmode == LockMode::INTENTION_EXCLUSIVE &&
               (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);

    } else if (txn_lockmode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    } else if (txn_lockmode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  // part4
  std::unique_lock<std::mutex> lock(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lockrequest = table_lock_map_[oid];
  lock.unlock();

  // part5
  std::unique_lock<std::mutex> queue_lock(lockrequest->latch_);
  if (already_lock) {
    if (lockrequest->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    lockrequest->upgrading_ = txn->GetTransactionId();
    auto it = lockrequest->request_queue_.begin();
    while (it != lockrequest->request_queue_.end()) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
        delete *it;
        lockrequest->request_queue_.erase(it);
        break;
      } else {
        it++;
      }
    }
    RemoveTableLockFromTxn(txn, txn_lockmode, oid);
  }
  // part6
  auto new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  if (already_lock) {
    auto it = lockrequest->request_queue_.begin();
    while (it != lockrequest->request_queue_.end() && (*it)->granted_) {
      it++;
    }
    lockrequest->request_queue_.insert(it, new_request);
  } else {
    lockrequest->request_queue_.push_back(new_request);
  }

  // part7
  while (!Grantlock(lockrequest->request_queue_, new_request)) {
    lockrequest->cv_.wait(queue_lock);

    if (txn->GetState() == TransactionState::ABORTED) {
      auto it = std::find(lockrequest->request_queue_.begin(), lockrequest->request_queue_.end(), new_request);
      if (it != lockrequest->request_queue_.end()) {
        lockrequest->request_queue_.erase(it);
        delete new_request;
      }

      if (already_lock) {
        lockrequest->upgrading_ = INVALID_TXN_ID;
      }
      lockrequest->cv_.notify_all();
      return false;
    }
  }
  new_request->granted_ = true;
  // part8
  if (already_lock) {
    lockrequest->upgrading_ = INVALID_TXN_ID;
  }
  // 将锁添加到事务的锁集合
  AddTableLockToTxn(txn, lock_mode, oid);

  // 唤醒其他等待的事务
  lockrequest->cv_.notify_all();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lockqueue = table_lock_map_[oid];
  table_map_lock.unlock();

  std::unique_lock<std::mutex> queuelock(lockqueue->latch_);
  bool find = false;
  LockRequest *target;
  LockMode lockmode;
  for (auto &lockrequest : lockqueue->request_queue_) {
    if (lockrequest->txn_id_ == txn->GetTransactionId() && lockrequest->granted_) {
      find = true;
      target = lockrequest;
      lockmode = lockrequest->lock_mode_;
    }
  }
  if (!find) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto share_lock_set = txn->GetSharedRowLockSet();
  if (share_lock_set->find(oid) != share_lock_set->end() && !(*share_lock_set)[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto exclusive_lock_set = txn->GetExclusiveRowLockSet();
  if (exclusive_lock_set->find(oid) != exclusive_lock_set->end() && !(*exclusive_lock_set)[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto it = lockqueue->request_queue_.begin();
  while (it != lockqueue->request_queue_.end()) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      delete *it;
      lockqueue->request_queue_.erase(it);
    }
  }
  if (lockmode == LockMode::SHARED) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
      txn->GetSharedTableLockSet()->erase(oid);
    }
  }
  else if (lockmode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
    txn->GetExclusiveTableLockSet()->erase(oid);
  }

  lockqueue->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // part1
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }
  bool has_required_table_lock = false;

  switch (lock_mode) {
    case LockMode::SHARED:
      // 行级 S 锁需要表级 IS, S, IX, SIX, X 锁
      has_required_table_lock = txn->IsTableIntentionSharedLocked(oid) || txn->IsTableSharedLocked(oid) ||
                                txn->IsTableIntentionExclusiveLocked(oid) ||
                                txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid);
      break;

    case LockMode::EXCLUSIVE:
      // 行级 X 锁需要表级 IX, SIX, X 锁
      has_required_table_lock = txn->IsTableIntentionExclusiveLocked(oid) ||
                                txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid);
      break;
  }

  if (!has_required_table_lock) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // part2
  LockMode current_mode;
  bool already_lock = false;
  if (txn->IsRowSharedLocked(oid, rid)) {
    already_lock = true;
    current_mode = LockMode::SHARED;
  } else if (txn->IsRowExclusiveLocked(oid, rid)) {
    already_lock = true;
    current_mode = LockMode::EXCLUSIVE;
  }

  // part3
  if (already_lock) {
    if (current_mode == lock_mode) {
      return true;
    } else if (current_mode == LockMode::SHARED && (lock_mode != LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    } else if (current_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  }
  // part4
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &requestqueue = row_lock_map_[rid];
  row_lock.unlock();

  // part5
  std::unique_lock<std::mutex> queue_lock(requestqueue->latch_);
  if (already_lock) {
    if (requestqueue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    requestqueue->upgrading_ = txn->GetTransactionId();
    auto it = requestqueue->request_queue_.begin();
    while (it != requestqueue->request_queue_.end()) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
        delete *it;
        requestqueue->request_queue_.erase(it);
        break;
      } else {
        it++;
      }
    }
    RemoveRowLockFromTxn(txn, current_mode, oid, rid);
  }

  // part6
  auto lockrequest = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  if (already_lock) {
    auto insert_pos = requestqueue->request_queue_.begin();
    while (insert_pos != requestqueue->request_queue_.end() && (*insert_pos)->granted_) {
      insert_pos++;
    }
    requestqueue->request_queue_.insert(insert_pos, lockrequest);
  } else {
    requestqueue->request_queue_.push_back(lockrequest);
  }
  // part7
  while (!Grantlock(requestqueue->request_queue_, lockrequest)) {
    requestqueue->cv_.wait(queue_lock);

    if (txn->GetState() == TransactionState::ABORTED) {
      auto it = std::find(requestqueue->request_queue_.begin(), requestqueue->request_queue_.end(), lockrequest);
      if (it != requestqueue->request_queue_.end()) {
        requestqueue->request_queue_.erase(it);
        delete lockrequest;
      }
      if (already_lock) {
        requestqueue->upgrading_ = INVALID_TXN_ID;
      }
      requestqueue->cv_.notify_all();
      return false;
    }
  }
  lockrequest->granted_ = true;

  // part8
  if (already_lock) {
    requestqueue->upgrading_ = INVALID_TXN_ID;
  }
  AddRowLockToTxn(txn, lock_mode, oid, rid);
  requestqueue->cv_.notify_all();

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub

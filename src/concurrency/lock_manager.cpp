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

auto LockManager::CheckLockCanUpgrade(LockMode lock_mode1, LockMode lock_mode2) -> bool {
  if (lock_mode1 == LockMode::INTENTION_SHARED) {
    return lock_mode2 == LockMode::EXCLUSIVE || lock_mode2 == LockMode::SHARED ||
           lock_mode2 == LockMode::INTENTION_EXCLUSIVE || lock_mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (lock_mode1 == LockMode::SHARED) {
    return lock_mode2 == LockMode::EXCLUSIVE || lock_mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (lock_mode1 == LockMode::INTENTION_EXCLUSIVE) {
    return lock_mode2 == LockMode::EXCLUSIVE || lock_mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (lock_mode1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return lock_mode2 == LockMode::EXCLUSIVE;
  }
  return false;
}

auto LockManager::CheckLockCanCompatible(LockMode lock_mode1, LockMode lock_mode2) -> bool {
  if (lock_mode1 == LockMode::INTENTION_SHARED) {
    return lock_mode2 == LockMode::INTENTION_SHARED || lock_mode2 == LockMode::INTENTION_EXCLUSIVE ||
           lock_mode2 == LockMode::SHARED || lock_mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (lock_mode1 == LockMode::INTENTION_EXCLUSIVE) {
    return lock_mode2 == LockMode::INTENTION_SHARED || lock_mode2 == LockMode::INTENTION_EXCLUSIVE;
  }
  if (lock_mode1 == LockMode::SHARED) {
    return lock_mode2 == LockMode::INTENTION_SHARED || lock_mode2 == LockMode::SHARED;
  }
  if (lock_mode1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return lock_mode2 == LockMode::INTENTION_SHARED;
  }
  return false;
}

auto LockManager::RemoveFromLockTableSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}
auto LockManager::AddToLockTableSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

void LockManager::AddIntoTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
}

void LockManager::RemoveTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }
}

auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, std::shared_ptr<LockRequestQueue> &lockRequestQueue)
    -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto it = lockRequestQueue->request_queue_.begin(); it != lockRequestQueue->request_queue_.end(); it++) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
        auto p = *it;
        lockRequestQueue->request_queue_.erase(it);
        delete p;
        return true;
      }
    }
  }

  for (auto &it : lockRequestQueue->request_queue_) {
    if (!CheckLockCanCompatible(it->lock_mode_, lock_mode) && it->granted_) {
      return false;
    }
  }

  if (lockRequestQueue->upgrading_ != INVALID_TXN_ID) {
    if (lockRequestQueue->upgrading_ == txn->GetTransactionId()) {
      for (auto it = lockRequestQueue->request_queue_.begin(); it != lockRequestQueue->request_queue_.end(); it++) {
        if (txn->GetTransactionId() == (*it)->txn_id_ && !(*it)->granted_) {
          lockRequestQueue->upgrading_ = INVALID_TXN_ID;
          (*it)->granted_ = true;
          return true;
        }
      }
      LOG_ERROR("升级锁的时候，锁已经被授权了 或者 再队列中没有找到这个请求");
      // throw Exception("升级锁的时候，锁已经被授权了 或者 再队列中没有找到这个请求");
      return false;
    }
    LOG_ERROR("升级锁的时候，当前事务id等于正在升级锁的事务id");
    return false;
  }

  int first_ungranted = 1;
  for (auto &it : lockRequestQueue->request_queue_) {
    if (it->granted_) {
      continue;
    }
    if (it->txn_id_ == txn->GetTransactionId() && (first_ungranted != 0)) {
      it->granted_ = true;
      return true;
    }

    if (!CheckLockCanCompatible(lock_mode, it->lock_mode_)) {
      return false;
    }
    // it->granted_ = true; // todo 兼容的话可以一起授予锁
  }

  LOG_ERROR("漏掉的某些情况导致默认return");
  return false;
}

auto LockManager::CheckAllRowsUnlock(Transaction *txn, const table_oid_t &oid) -> bool {
  row_lock_map_latch_.lock();
  // todo 或许这里map能提前解锁
  for (auto &p : row_lock_map_) {
    auto lrq = p.second;
    lrq->latch_.lock();

    for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); it++) {
      if ((*it)->granted_ && (*it)->oid_ == oid && (*it)->txn_id_ == txn->GetTransactionId()) {
        row_lock_map_latch_.unlock();
        lrq->latch_.unlock();
        return false;
      }
    }
    lrq->latch_.unlock();
  }

  row_lock_map_latch_.unlock();
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    LOG_ERROR("逻辑异常1");
    //  throw Exception("逻辑异常1");
    return false;
  }

  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        LOG_ERROR("Growing阶段RU隔离级别下不允许锁类型");
        throw Exception("Growing阶段RU隔离级别下不允许锁类型");
      }
    }
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      LOG_ERROR("SHRINKING阶段RU隔离级别下不允许获取锁");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      // SHRINKING阶段可以获取短期的读锁
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        LOG_ERROR("SHRINKING阶段RC隔离级别下不允许获取锁类型");
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      LOG_ERROR("SHRINKING阶段RR隔离级别下不允许获取锁类型");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // LOG_INFO("QAQ");
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    std::shared_ptr<LockRequestQueue> lock_request_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = lock_request_queue;
  }
  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  int flag = 0;
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    auto p = *it;
    if (p->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    // 队列里有这个事务对锁请求
    if (lock_mode == p->lock_mode_) {
      // 请求类型相同且已经授权 返回
      return true;
    }
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      // todo 当前实现不允许同步升级，如果新增一条升级队列是可以的？
      // 此时正在锁升级
      txn->SetState(TransactionState::ABORTED);
      LOG_ERROR("此时正在锁升级");
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    if (!CheckLockCanUpgrade(p->lock_mode_, lock_mode)) {
      // 锁升级不兼容
      txn->SetState(TransactionState::ABORTED);
      LOG_ERROR("锁升级不兼容");
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // 释放当前已经持有的锁，并在queue中标记我正在升级

    lock_request_queue->upgrading_ = txn->GetTransactionId();
    RemoveFromLockTableSet(txn, (*it)->lock_mode_, oid);
    lock_request_queue->request_queue_.erase(it);
    delete p;
    lock_request_queue->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
    flag = 1;
    break;
  }

  if (flag == 0) {
    lock_request_queue->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
  }

  while (!GrantLock(txn, lock_mode, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    lock_request_queue->cv_.notify_all();
    return false;
  }

  AddToLockTableSet(txn, lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  //    todo 这里为什么不能这样return
  //  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
  //    LOG_ERROR("逻辑异常3");
  //    // throw Exception("逻辑异常3");
  //    txn->SetState(TransactionState::ABORTED);
  //    return false;
  //  }

  if (!CheckAllRowsUnlock(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    std::shared_ptr<LockRequestQueue> lock_request_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = lock_request_queue;
  }
  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if (!(*it)->granted_) {
      LOG_ERROR("解锁时候找到了对应事务，但是没有被授予锁");
      return false;
    }
    auto p = *it;
    RemoveFromLockTableSet(txn, (*it)->lock_mode_, oid);
    lock_request_queue->request_queue_.erase(it);

    lock_request_queue->cv_.notify_all();

    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (p->lock_mode_ == LockMode::EXCLUSIVE || p->lock_mode_ == LockMode::SHARED) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (p->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (p->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }

    delete p;
    return true;
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    LOG_ERROR("逻辑异常4");
    // throw Exception("逻辑异常4");
    return false;
  }

  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      LOG_ERROR("ROW-SHRINKING阶段RU隔离级别下不允许获取锁");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      // SHRINKING阶段可以获取短期的读锁
      if (lock_mode != LockMode::SHARED) {
        LOG_ERROR("ROW-SHRINKING阶段RC隔离级别下不允许获取锁类型");
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      LOG_ERROR("ROW-SHRINKING阶段RR隔离级别下不允许获取锁类型");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  int flag = 0;
  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); it++) {
    if ((*it)->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if ((*it)->lock_mode_ == lock_mode) {
      return true;
    }
    if (lrq->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      LOG_ERROR("2此时正在锁升级");
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    if (!CheckLockCanUpgrade((*it)->lock_mode_, lock_mode)) {
      // 锁升级不兼容
      txn->SetState(TransactionState::ABORTED);
      LOG_ERROR("锁升级不兼容");
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    auto p = *it;

    lrq->upgrading_ = txn->GetTransactionId();
    RemoveTxnRowLockSet(txn, (*it)->lock_mode_, oid, rid);
    lrq->request_queue_.erase(it);
    delete p;
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
    flag = 1;
    break;
  }

  if (flag == 0) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
  }

  while (!GrantLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    lrq->cv_.notify_all();
    return false;
  }
  AddIntoTxnRowLockSet(txn, lock_mode, oid, rid);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // todo 这里为什么不能这样return
  //  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
  //    LOG_ERROR("逻辑异常5");
  //    // throw Exception("逻辑异常5");
  //    return false;
  //  }

  row_lock_map_latch_.lock();
  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); it++) {
    if ((*it)->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if (!(*it)->granted_) {
      LOG_ERROR("解锁时候找到了对应事务，但是没有被授予锁");
      return false;
    }
    if (!force) {
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if ((*it)->lock_mode_ == LockMode::EXCLUSIVE || (*it)->lock_mode_ == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        if ((*it)->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if ((*it)->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
    }

    auto p = *it;
    RemoveTxnRowLockSet(txn, (*it)->lock_mode_, oid, rid);
    lrq->request_queue_.erase(it);

    lrq->cv_.notify_all();

    delete p;
    return true;
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

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

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

auto LockManager::IsTableLocked(Transaction *txn, const table_oid_t &oid, const std::vector<LockMode> &lock_modes)
    -> std::optional<LockMode> {
  std::optional<LockMode> mode = std::nullopt;
  for (const auto &lock_mode : lock_modes) {
    switch (lock_mode) {
      case LockMode::SHARED:
        if (txn->IsTableSharedLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::SHARED);
        }
        break;
      case LockMode::EXCLUSIVE:
        if (txn->IsTableExclusiveLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::EXCLUSIVE);
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (txn->IsTableIntentionExclusiveLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::INTENTION_EXCLUSIVE);
        }
        break;
      case LockMode::INTENTION_SHARED:
        if (txn->IsTableIntentionSharedLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::INTENTION_SHARED);
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::SHARED_INTENTION_EXCLUSIVE);
        }
        break;
    }
    if (mode.has_value()) {
      return mode;
    }
  }
  return std::nullopt;
}

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
    // AddToLockTableSet(txn, lock_mode, oid);
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    // RemoveFromLockTableSet(txn, lock_mode, oid);
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
        if (p->txn_id_ == lockRequestQueue->upgrading_) {
          lockRequestQueue->upgrading_ = INVALID_TXN_ID;
        }
        delete p;
        LOG_ERROR("txn变成aborted状态,且遍历能够找到且删除");
        return true;
      }
    }
    LOG_ERROR("txn变成aborted状态,且遍历未找到");
    return true;
  }

  LOG_INFO("准备授权 LOCK TABLE /n, txnID = %d, LOCkMODE = %d", txn->GetTransactionId(), (int)lock_mode);

  for (auto &it : lockRequestQueue->request_queue_) {
    if (!CheckLockCanCompatible(it->lock_mode_, lock_mode) && it->granted_) {
      LOG_ERROR("11锁升级不兼容已经授权的 LOCK TABLE /n, txnID = %d, LOCkMODE = %d", txn->GetTransactionId(),
                (int)lock_mode);
      LOG_ERROR("22锁升级不兼容已经授权的 LOCK TABLE /n, txnID = %d, LOCkMODE = %d", (*it).txn_id_, (int)lock_mode);
      return false;
    }
  }

  if (lockRequestQueue->upgrading_ != INVALID_TXN_ID) {
    int de1 = lockRequestQueue->upgrading_;
    if (lockRequestQueue->upgrading_ == txn->GetTransactionId()) {
      lockRequestQueue->upgrading_ = INVALID_TXN_ID;
      for (auto &it : lockRequestQueue->request_queue_) {
        if (txn->GetTransactionId() == it->txn_id_ && !it->granted_) {
          // lockRequestQueue->upgrading_ = INVALID_TXN_ID;
          it->granted_ = true;
          LOG_INFO("锁升级成功 事务id , txnID = %d, LOCkMODE = %d", txn->GetTransactionId(), (int)lock_mode);
          return true;
        }
      }
      LOG_ERROR("升级锁的时候，锁已经被授权了 或者 再队列中没有找到这个请求");
      throw Exception("升级锁的时候，锁已经被授权了 或者 再队列中没有找到这个请求");
      return false;
    }
    LOG_ERROR("正在升级锁的事务id , txnID = %d, LOCkMODE = %d", de1, (int)lock_mode);

    LOG_ERROR("升级锁的时候，当前事务id不等于正在升级锁的事务id");
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
    it->granted_ = true;  // todo 兼容的话可以一起授予锁
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
        lrq->latch_.unlock();
        row_lock_map_latch_.unlock();
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
        txn->SetState(TransactionState::ABORTED);
        LOG_ERROR("Growing阶段RU隔离级别下不允许锁类型");
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      LOG_ERROR("SHRINKING阶段RU隔离级别下不允许获取锁");
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      // SHRINKING阶段可以获取短期的读锁
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        LOG_ERROR("SHRINKING阶段RC隔离级别下不允许获取锁类型");
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      LOG_ERROR("SHRINKING阶段RR隔离级别下不允许获取锁类型");
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  LOG_INFO("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d", txn->GetTransactionId(), oid, (int)lock_mode);

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

    if (!CheckLockCanUpgrade(p->lock_mode_, lock_mode)) {
      // 锁升级不兼容
      LOG_ERROR("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d 锁升级不兼容", txn->GetTransactionId(), oid,
                (int)lock_mode);
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      // todo 当前实现不允许同步升级，如果新增一条升级队列是可以的？
      // 此时正在锁升级
      LOG_ERROR("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d 此时锁正在升级", txn->GetTransactionId(), oid,
                (int)lock_mode);
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // 释放当前已经持有的锁，并在queue中标记我正在升级
    LOG_INFO("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d 升级这个锁", txn->GetTransactionId(), oid,
             (int)lock_mode);
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    lock_request_queue->request_queue_.erase(it);
    RemoveFromLockTableSet(txn, p->lock_mode_, oid);
    delete p;

    for (auto it2 = lock_request_queue->request_queue_.begin(); it2 != lock_request_queue->request_queue_.end();
         it2++) {
      if (!(*it2)->granted_) {
        lock_request_queue->request_queue_.insert(it2, new LockRequest(txn->GetTransactionId(), lock_mode, oid));
        flag = 1;
        break;
      }
    }

    // lock_request_queue->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));

    break;
  }

  if (flag == 0) {
    lock_request_queue->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
  }
  LOG_INFO("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d 开始等待", txn->GetTransactionId(), oid, (int)lock_mode);

  while (!GrantLock(txn, lock_mode, lock_request_queue)) {
    // printf("XUNHUAN:  --- id: %d  Mode：%d ck2\n", txn->GetTransactionId(),(int)lock_mode);
    lock_request_queue->cv_.wait(lock);
  }
  LOG_INFO("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d 结束等待", txn->GetTransactionId(), oid, (int)lock_mode);
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d 获得锁失败", txn->GetTransactionId(), oid,
             (int)lock_mode);
    lock_request_queue->cv_.notify_all();
    return false;
  }
  LOG_INFO("LOCK TABLE /n, txnID = %d,  oid = %d LOCkMODE = %d 获得锁成功", txn->GetTransactionId(), oid,
           (int)lock_mode);
  AddToLockTableSet(txn, lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!CheckAllRowsUnlock(txn, oid)) {
    LOG_ERROR("有行锁没有释放");
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  LOG_INFO("UNLOCK TABLE /n, txnID = %d,  oid = %d", txn->GetTransactionId(), oid);
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    std::shared_ptr<LockRequestQueue> lock_request_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = lock_request_queue;
    LOG_ERROR("unlock table的时候没找到队列");
  }
  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if (!(*it)->granted_) {
      throw Exception("解锁时候找到了对应事务，但是没有被授予锁");
      LOG_ERROR("解锁时候找到了对应事务，但是没有被授予锁");
      continue;
    }
    auto p = *it;

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
    LOG_INFO("UNLOCK TABLE 成功 !  /n, txnID = %d,  oid = %d ,lockmode = %d", txn->GetTransactionId(), oid,
             (int)p->lock_mode_);

    RemoveFromLockTableSet(txn, p->lock_mode_, oid);
    lock_request_queue->request_queue_.erase(it);
    delete p;
    lock_request_queue->cv_.notify_all();

    return true;
  }
  LOG_INFO("UNLOCK TABLE 失败 !  /n, txnID = %d,  oid = %d ", txn->GetTransactionId(), oid);
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
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED) {
      LOG_ERROR("ROW-GROWING && R-UN不给SHARD锁？");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
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

  // todo 检查是否持有 row 对应的 table lock。必须先持有 table lock 再持有 row lock。

  if (lock_mode == LockMode::SHARED) {
    // 表可以持有任意类型的锁
    std::vector<LockMode> locks = {LockMode::EXCLUSIVE, LockMode::SHARED, LockMode::INTENTION_SHARED,
                                   LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE};
    if (IsTableLocked(txn, oid, locks) == std::nullopt) {
      LOG_ERROR("未先获取表锁1 尝试获取表锁");
      bool flag = false;
      for (auto &lock : locks) {
        flag = LockTable(txn, lock, oid);
        if (flag) {
          break;
        }
      }
      if (!flag) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    std::vector<LockMode> locks = {LockMode::EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE,
                                   LockMode::INTENTION_EXCLUSIVE};
    if (IsTableLocked(txn, oid, locks) == std::nullopt) {
      LOG_ERROR("未先获取表锁2");
      bool flag = false;
      for (auto &lock : locks) {
        flag = LockTable(txn, lock, oid);
        if (flag) {
          break;
        }
      }
      if (flag) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
    }
  } else {
    throw Exception("lock row中有意料之外的锁类型");
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();
  LOG_INFO("LOCK ROW /n, txnID = %d,  oid = %d ,RID = %d,LOCkMODE = %d", txn->GetTransactionId(), oid, (int)rid.Get(),
           (int)lock_mode);
  int flag = 0;
  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); it++) {
    if ((*it)->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if ((*it)->lock_mode_ == lock_mode) {
      return true;
    }
    if (!CheckLockCanUpgrade((*it)->lock_mode_, lock_mode)) {
      // 锁升级不兼容
      txn->SetState(TransactionState::ABORTED);
      LOG_ERROR("LOCK ROW /n, txnID = %d,  oid = %d ,RID = %d,LOCkMODE = %d 锁升级不兼容", txn->GetTransactionId(), oid,
                (int)rid.Get(), (int)lock_mode);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    if (lrq->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      LOG_INFO("LOCK ROW /n, txnID = %d,  oid = %d ,RID = %d,LOCkMODE = %d 锁正在升级", txn->GetTransactionId(), oid,
               (int)rid.Get(), (int)lock_mode);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    auto p = *it;

    lrq->upgrading_ = txn->GetTransactionId();
    RemoveTxnRowLockSet(txn, (*it)->lock_mode_, oid, rid);
    lrq->request_queue_.erase(it);
    delete p;

    for (auto it2 = lrq->request_queue_.begin(); it2 != lrq->request_queue_.end(); it2++) {
      if (!(*it2)->granted_) {
        lrq->request_queue_.insert(it2, new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid));
        flag = 1;
        break;
      }
    }

    break;
  }

  if (flag == 0) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid));
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
  if (!txn->IsRowExclusiveLocked(oid, rid) && !txn->IsRowSharedLocked(oid, rid)) {
    txn->SetState(TransactionState::ABORTED);
    LOG_ERROR("UNLOCK ROW /n, txnID = %d,  oid = %d ,RID = %d,解锁行锁，但是之前没有锁", txn->GetTransactionId(), oid,
              (int)rid.Get());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); it++) {
    if ((*it)->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if (!(*it)->granted_) {
      LOG_ERROR("UNLOCKROW解锁时候找到了对应事务，但是没有被授予锁");
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

  for (auto &p : row_lock_map_) {
    for (auto pp : p.second->request_queue_) {
      delete pp;
    }
    p.second->request_queue_.clear();
  }
  row_lock_map_.clear();
  for (auto &p : table_lock_map_) {
    for (auto pp : p.second->request_queue_) {
      delete pp;
    }
    p.second->request_queue_.clear();
  }
  table_lock_map_.clear();
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (txn_manager_->GetTransaction(t1)->GetState() != TransactionState::ABORTED) {
    if (txn_manager_->GetTransaction(t2)->GetState() != TransactionState::ABORTED) {
      waits_for_[t1].push_back(t2);
    }
  }
  std::sort(waits_for_[t1].begin(), waits_for_[t1].end());
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if ((*it) == t2) {
      waits_for_[t1].erase(it);
      break;
    }
  }
  waits_for_latch_.unlock();
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<std::pair<txn_id_t, txn_id_t>> edge = GetEdgeList();

  if (edge.empty()) {
    return false;
  }

  return dfs(edge[0].first, txn_id);
}

void dfs(txn_id_t now, txn_id_t *txn_id) {
  if (vis[now]) {
    // 有环
    txn_id = return true;
  }
  vis[now] = true;
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if (vis[*it] == false) {
      return
    }
  }
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  waits_for_latch_.lock();
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto pa : waits_for_) {
    for (auto p : pa.second) {
      edges.push_back({pa.first, p});
    }
  }
  std::sort(edges.begin(), edges.end());
  waits_for_latch_.unlock();
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

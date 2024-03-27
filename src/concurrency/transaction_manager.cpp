//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  while (!txn->GetWriteSet()->empty()) {
    TableWriteRecord twr = txn->GetWriteSet()->back();
    if (twr.wtype_ == WType::INSERT) {
      TupleMeta tp = twr.table_heap_->GetTupleMeta(twr.rid_);
      tp.is_deleted_ = true;
      twr.table_heap_->UpdateTupleMeta(tp, twr.rid_);
    } else if (twr.wtype_ == WType::DELETE) {
      TupleMeta tp = twr.table_heap_->GetTupleMeta(twr.rid_);
      tp.is_deleted_ = false;
      twr.table_heap_->UpdateTupleMeta(tp, twr.rid_);
    } else if (twr.wtype_ == WType::UPDATE) {
      // twr.table_heap_->UpdateTupleInPlaceUnsafe(twr.old_tuple_meta_, twr.old_tuple_, twr.rid_);
      LOG_ERROR("UPdate trans");
    }
    txn->GetWriteSet()->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub

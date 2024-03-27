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
#include "common/logger.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}
SeqScanExecutor::~SeqScanExecutor() {
  if (table_iterator_ != nullptr) {
    delete table_iterator_;
    table_iterator_ = nullptr;
  }
  LOG_INFO("F");
}
void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");

  if (exec_ctx_->IsDelete()) {
    try {
      bool success = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
      if (!success) {
        LOG_ERROR("seq算子锁表失败");
      }
    } catch (TransactionAbortException &e) {
    }
  } else {
    auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
    if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
      try {
        bool success = exec_ctx_->GetLockManager()->LockTable(
            exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
        if (!success) {
          LOG_ERROR("#seq算子锁表失败#");
        }
      } catch (TransactionAbortException &e) {
      }
    }
  }

  LOG_INFO("Seq INIT ...");
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->table_oid_);

  delete table_iterator_;

  table_iterator_ =
      new TableIterator(table_info->table_->MakeEagerIterator());  // 这里返回的是一个临时的对象，自动调用移动构造函数
  if (table_iterator_ == nullptr) {
    throw Exception("异常0010table_iterator_ = nullptr");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_INFO("调用Next");
  // LOG_INFO("is delete %s", std::to_string(tuple1.first.is_deleted_).c_str());  //
  // std::to_string(key.ToString()).c_str()
  //  std::cout<<"dizhi : "<<&(table_iterator_)<<std::endl;
  // LOG_INFO("Seq Next ...");
  if (table_iterator_ == nullptr) {
    throw Exception("异常0table_iterator_ = NULL");
  }

  while (true) {
    if (table_iterator_->IsEnd()) {
      break;
    }
    std::pair<TupleMeta, Tuple> tuple1 = table_iterator_->GetTuple();

    if (exec_ctx_->IsDelete()) {
      try {
        auto success = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, tuple->GetRid());
        if (!success) {
          LOG_ERROR("#seq算子Next锁Row(Ex)失败#");
        }
      } catch (TransactionAbortException &e) {
      }
    } else {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
        try {
          auto success = exec_ctx_->GetLockManager()->LockRow(
              exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::SHARED, plan_->table_oid_, tuple->GetRid());
          if (!success) {
            LOG_ERROR("#seq算子Next锁Row (Shared)失败#");
          }
        } catch (TransactionAbortException &e) {
        }
      }
    }

    // LOG_INFO("search ...!");
    if (!tuple1.first.is_deleted_) {
      *tuple = tuple1.second;  // copy
      if (tuple1.second.GetRid() == table_iterator_->GetRID()) {
        // LOG_INFO("search successful!");
      } else {
        throw Exception("rid buxiang deng");
      }
      *rid = tuple1.second.GetRid();  // original
      ++(*table_iterator_);
      // LOG_INFO("search successful!");
      return true;
    }

    try {
      bool success =
          exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, tuple->GetRid(), true);
      if (!success) {
        LOG_ERROR("#seq算子Next强制解锁Row 失败#");
      }
    } catch (TransactionAbortException &e) {
    }

    ++(*table_iterator_);
    if (table_iterator_->IsEnd()) {
      break;
    }
  }
  return false;
}

}  // namespace bustub

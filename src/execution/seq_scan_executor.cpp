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
  LOG_INFO("Seq INIT ...");
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->table_oid_);

  delete table_iterator_;

  table_iterator_ =
      new TableIterator(table_info->table_->MakeIterator());  // 这里返回的是一个临时的对象，自动调用移动构造函数
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
    ++(*table_iterator_);
    if (table_iterator_->IsEnd()) {
      break;
    }
  }
  return false;
}

}  // namespace bustub

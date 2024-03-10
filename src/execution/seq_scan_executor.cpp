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
}
void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(table_oid);

  delete table_iterator_;

  table_iterator_ =
      new TableIterator(table_info->table_->MakeEagerIterator());  // 这里返回的是一个临时的对象，自动调用移动构造函数
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_INFO("调用Next");
  // LOG_INFO("is delete %s", std::to_string(tuple1.first.is_deleted_).c_str());  //
  // std::to_string(key.ToString()).c_str()
  while (true) {
    if (table_iterator_->IsEnd()) {
      break;
    }
    std::pair<TupleMeta, Tuple> tuple1 = table_iterator_->GetTuple();
    if (!tuple1.first.is_deleted_) {
      *tuple = tuple1.second;         // copy
      *rid = tuple1.second.GetRid();  // original
      ++(*table_iterator_);
      return true;
    }
    ++(*table_iterator_);
    tuple1 = table_iterator_->GetTuple();
  }
  return false;
}

}  // namespace bustub

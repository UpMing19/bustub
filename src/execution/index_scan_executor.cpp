//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      catalog_(exec_ctx_->GetCatalog()),
      index_info_(catalog_->GetIndex(plan_->GetIndexOid())),
      table_info_(catalog_->GetTable(index_info_->table_name_)),
      tree_it_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
      it_(tree_it_->GetBeginIterator()),
      end_it_(tree_it_->GetEndIterator()) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
  end_flag_ = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (end_flag_) {
    return false;
  }

  for (;;) {
    if (it_ == end_it_) {
      return false;
    }
    auto mapping_type = *it_;
    RID res_rid = mapping_type.second;
    std::pair<TupleMeta, Tuple> res_tuple = table_info_->table_->GetTuple(res_rid);
    if (res_tuple.first.is_deleted_) {
      ++it_;
    } else {
      *tuple = res_tuple.second;
      *rid = res_rid;
      ++it_;
      return true;
    }
  }

  end_flag_ = true;
  return true;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  sort_vec_.clear();
  now_ = 0;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sort_vec_.push_back(tuple);
  }
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (now_ == plan_->GetLimit()) {
    return false;
  }
  if (now_ == sort_vec_.size()) {
    return false;
  }

  *tuple = sort_vec_[now_];
  *rid = tuple->GetRid();
  now_++;
  return true;
}

}  // namespace bustub

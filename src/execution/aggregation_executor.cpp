//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/logger.h"
#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  LOG_INFO("agg init");
  aht_.Clear();
  child_->Init();
  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    flag_ = 1;
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  // if (flag_ == 0) {
  //   LOG_INFO("空表插入一个");
  //   aht_.InsertCombine((AggregateKey){std::vector<Value>{}}, aht_.GenerateInitialAggregateValue());
  // }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (flag_ == 0) {
    if (!plan_->group_bys_.empty()) {
      return false;
    }
    flag_ = 1;

    std::vector<Value> values;
    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema());
    *rid = tuple->GetRid();
    return true;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  values = aht_iterator_.Key().group_bys_;
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple(values, &plan_->OutputSchema());
  *rid = tuple->GetRid();
  ++(aht_iterator_);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  LOG_INFO("hash join build...");
}

void HashJoinExecutor::Init() {
  LOG_INFO("hash join Init...");
  ht_.clear();
  match_vec_.clear();
  left_child_->Init();
  right_child_->Init();
  std::cout << " LeftJoinKeyExpressions[0] plan " << plan_->LeftJoinKeyExpressions()[0] << '\n';
  std::cout << " RightJoinKeyExpressions[0] plan " << plan_->RightJoinKeyExpressions()[0] << '\n';
  Tuple tuple;
  RID r;

  while (right_child_->Next(&tuple, &r)) {
    JoinKey join_key;
    for (auto &p : plan_->RightJoinKeyExpressions()) {
      join_key.join_keys_.push_back(p->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }
    ht_[join_key].join_values_.push_back(tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (match_num_ != static_cast<int>(match_vec_.size())) {
      throw Exception("异常size不想等");
    }
    if (match_num_ != 0) {  // 输出之前的匹配结果
      match_num_--;
      *tuple = *match_vec_.begin();
      *rid = tuple->GetRid();
      match_vec_.erase(match_vec_.begin());
      return true;
    }
    if (!left_child_->Next(tuple, rid)) {
      return false;
    }

    JoinKey join_key;
    for (auto &p : plan_->LeftJoinKeyExpressions()) {
      join_key.join_keys_.push_back(p->Evaluate(tuple, left_child_->GetOutputSchema()));
    }

    if (ht_.count(join_key) == 0 &&
        plan_->GetJoinType() == JoinType::LEFT) {  // 没有匹配到且还是left join 需要输出一个空
      match_vec_.clear();
      match_num_ = 1;

      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(tuple->GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      Tuple match_tuple = Tuple{values, &GetOutputSchema()};
      match_vec_.push_back(match_tuple);
    } else if (ht_.count(join_key) != 0) {  // 这里匹配结果可能有多个，一个一个输出
      match_vec_.clear();

      for (const auto &right_tuple : ht_[join_key].join_values_) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(tuple->GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
        }
        Tuple match_tuple = Tuple{values, &GetOutputSchema()};

        match_vec_.push_back(match_tuple);
      }

      match_num_ = match_vec_.size();
    }
  }

  return false;
}

}  // namespace bustub

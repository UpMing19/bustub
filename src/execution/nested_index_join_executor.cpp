//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  LOG_ERROR("nested index join build");
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  right_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  left_temp_tuple_ = new Tuple();
  RID r;
  child_executor_->Next(left_temp_tuple_, &r);
  flag_ = 0;
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (true) {
      std::vector<Value> values{plan_->key_predicate_->Evaluate(left_temp_tuple_, child_executor_->GetOutputSchema())};
      Tuple tu = Tuple{values, index_info_->index_->GetKeySchema()};
      std::vector<RID> result;
      index_info_->index_->ScanKey(tu, &result, exec_ctx_->GetTransaction());

      if (!result.empty()) {
        std::vector<Value> value{};
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.push_back(left_temp_tuple_->GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_table_info_->schema_.GetColumnCount(); i++) {
          value.push_back(tu.GetValue(&right_table_info_->schema_, i));
        }
        Tuple output_tuple = {value, &plan_->OutputSchema()};
        *tuple = output_tuple;
        flag_ = 1;
        return true;
      }

      if (flag_ == 0) {
        std::vector<Value> value{};
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.push_back(left_temp_tuple_->GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_table_info_->schema_.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_table_info_->schema_.GetColumn(i).GetType()));
        }
        Tuple output_tuple = {value, &plan_->OutputSchema()};
        *tuple = output_tuple;
      }

      if (!child_executor_->Next(left_temp_tuple_, rid)) {
        delete left_temp_tuple_;
        break;
      }
      flag_ = 0;
    }

  } else {
    while (true) {
      std::vector<Value> values{plan_->key_predicate_->Evaluate(left_temp_tuple_, child_executor_->GetOutputSchema())};
      Tuple tu = Tuple{values, index_info_->index_->GetKeySchema()};
      std::vector<RID> result;
      index_info_->index_->ScanKey(tu, &result, exec_ctx_->GetTransaction());

      if (!result.empty()) {
        std::vector<Value> value{};
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.push_back(left_temp_tuple_->GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_table_info_->schema_.GetColumnCount(); i++) {
          value.push_back(tu.GetValue(&right_table_info_->schema_, i));
        }
        Tuple output_tuple = {value, &plan_->OutputSchema()};
        *tuple = output_tuple;
        return true;
      }
      if (!child_executor_->Next(left_temp_tuple_, rid)) {
        delete left_temp_tuple_;
        break;
      }
    }
  }

  return false;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  flag_=0;
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  LOG_INFO("join join init ");
  right_executor_->Init();
  left_executor_->Init();
  RID r ;
  LOG_INFO("join join init1 ");
  temp_left_ = new Tuple();
  rt_ = new Tuple();
  bool res  = left_executor_->Next(temp_left_, &r);
  std::cout<<"## : "<<res<<std::endl;
  flag_ = 0;
  LOG_INFO("join join init2 ");
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (true) {
      while (right_executor_->Next(rt_, rid)) {
        Value res = plan_->predicate_->EvaluateJoin(temp_left_, left_executor_->GetOutputSchema(), rt_,
                                                    right_executor_->GetOutputSchema());
        if (res.GetAs<bool>()) {
          std::vector<Value> values;

          for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(temp_left_->GetValue(&left_executor_->GetOutputSchema(), i));
          }

          for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(rt_->GetValue(&right_executor_->GetOutputSchema(), i));
          }

          Tuple output_tuple = Tuple{values, &GetOutputSchema()};
          *tuple = output_tuple;
          flag_ = 1;
          return true;
        }
      }
      if (flag_ == 0) {
        LOG_DEBUG("此次leftjoin左表未查找到对应右表内容");
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(temp_left_->GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        Tuple output_tuple = Tuple{values, &GetOutputSchema()};
        *tuple = output_tuple;
        flag_ = 1;
         { right_executor_->Init(); } //这里之所以用同一条目重新查一边右表是为了能够break掉循环 return false终止查询
         return true;
      }

      if (!left_executor_->Next(temp_left_, rid)) {
        if(temp_left_!= nullptr){
          delete temp_left_;
          temp_left_ = nullptr;
        }
        if(rt_!= nullptr){
          delete rt_;
          rt_ = nullptr;
        }
        break;
      }
      flag_ = 0;
      right_executor_->Init();
    }

  } else {

    while (true) {

      while (right_executor_->Next(rt_, rid)) {
        Value res = plan_->predicate_->EvaluateJoin(temp_left_, left_executor_->GetOutputSchema(), rt_,
                                                    right_executor_->GetOutputSchema());
        if (res.GetAs<bool>()) {
          std::vector<Value> values;

          for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(temp_left_->GetValue(&left_executor_->GetOutputSchema(), i));
          }

          for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(rt_->GetValue(&right_executor_->GetOutputSchema(), i));
          }

          Tuple output_tuple = Tuple{values, &GetOutputSchema()};
          *tuple = output_tuple;
          return true;
        }
      }


      if (!left_executor_->Next(temp_left_, rid)) {
        if(temp_left_!= nullptr){
          delete temp_left_;
          temp_left_ = nullptr;
        }
        if(rt_!= nullptr){
          delete rt_;
          rt_ = nullptr;
        }
        break;
      }

      right_executor_->Init();




    }
  }

  return false;
}

}  // namespace bustub

#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  now_ = 0;
  top_vec_.clear();
  auto cmp = [&](const Tuple t1, const Tuple t2) -> bool {
    for (const std::pair<OrderByType, AbstractExpressionRef> &p : plan_->GetOrderBy()) {
      Value v1 = p.second->Evaluate(&t1, child_executor_->GetOutputSchema());
      Value v2 = p.second->Evaluate(&t2, child_executor_->GetOutputSchema());
      if (OrderByType::DESC == p.first) {
        if (v2.CompareEquals(v1) != CmpBool::CmpTrue) {
          CmpBool c = v2.CompareLessThan(v1);
          return c == CmpBool::CmpTrue;
        }
      }
      if (v2.CompareEquals(v1) != CmpBool::CmpTrue) {
        CmpBool c = v2.CompareLessThan(v1);
        return c != CmpBool::CmpTrue;
      }
    }
    return true;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> top_n_queue(cmp);

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    top_n_queue.push(tuple);
    now_++;
    if (now_ > plan_->GetN()) {
      top_n_queue.pop();
      now_--;
    }
  }

  while (!top_n_queue.empty()) {
    top_vec_.push_back(top_n_queue.top());
    top_n_queue.pop();
  }
  std::reverse(top_vec_.begin(), top_vec_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (now_ == 0) {
    return false;
  }
  *tuple = top_vec_.back();
  *rid = tuple->GetRid();
  top_vec_.pop_back();
  now_--;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return now_; };

}  // namespace bustub

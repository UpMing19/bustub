#include "execution/executors/sort_executor.h"
#include "common/logger.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  now_ = 0;
}

void SortExecutor::Init() {
  child_executor_->Init();
  sort_vec_.clear();
  LOG_DEBUG("SortExecutor Init ...");
  now_ = 0;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sort_vec_.push_back(tuple);
  }

  std::sort(sort_vec_.begin(), sort_vec_.end(), [&](const Tuple t1, const Tuple t2) {
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
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (now_ == static_cast<uint64_t>(sort_vec_.size())) {
    return false;
  }
  *tuple = sort_vec_[now_];
  *rid = tuple->GetRid();
  now_++;
  return true;
}

}  // namespace bustub

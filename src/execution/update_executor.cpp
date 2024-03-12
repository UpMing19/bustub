//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  end_flag_ = false;
}

// UpdateExecutor::~UpdateExecutor() {
//   if (table_info_ != nullptr) {
//     delete table_info_;
//     table_info_ = nullptr;
//   }
// }

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  Catalog *c = exec_ctx_->GetCatalog();
  delete table_info_;
  table_info_ = c->GetTable(plan_->table_oid_);
  end_flag_ = false;
  child_executor_->Init();

  LOG_DEBUG("table_info_->schema_ expression: %s", table_info_->schema_.ToString().c_str());
  LOG_DEBUG("child_executor_->GetOutputSchema expression: %s", child_executor_->GetOutputSchema().ToString().c_str());
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (end_flag_) {
    return false;
  }
  Catalog *catalog = exec_ctx_->GetCatalog();
  int affect_cow = 0;
  LOG_INFO("update begin");
  while (child_executor_->Next(tuple, rid)) {
    Tuple remove_tuple = *tuple;
    LOG_DEBUG("remove tuple: %s", remove_tuple.ToString(&child_executor_->GetOutputSchema()).c_str());
    LOG_DEBUG("rid of remove tuple: %s", rid->ToString().c_str());
    std::pair<TupleMeta, Tuple> res_tuple = table_info_->table_->GetTuple(*rid);
    TupleMeta res_tuple_meta = res_tuple.first;
    res_tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(res_tuple_meta, *rid);

    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    Tuple update_tuple = *tuple;
    for (const auto &expr : plan_->target_expressions_) {
      LOG_DEBUG("target expression: %s", expr->ToString().c_str());
      values.push_back(expr->Evaluate(&update_tuple, child_executor_->GetOutputSchema()));
    }
    update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    // LOG_INFO("inser beg");
    std::optional<RID> r =
        table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, update_tuple,
                                         exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());

    // LOG_INFO("inser fin");

    if (!r.has_value()) {
      // LOG_ERROR("r has no value");
      return false;
    }
    // *rid = r.value();
    LOG_DEBUG("insert tuple: %s", tuple->ToString(&child_executor_->GetOutputSchema()).c_str());
    LOG_DEBUG("rid of inserted tuple: %s", rid->ToString().c_str());
    LOG_DEBUG("only child of the update node:\n %s\n", plan_->GetChildPlan()->ToString().c_str());

    // 更新索引

    std::vector<IndexInfo *> v = catalog->GetTableIndexes(table_info_->name_);
    for (auto p : v) {
      Tuple key = remove_tuple.KeyFromTuple(table_info_->schema_, p->key_schema_, p->index_->GetKeyAttrs());
      p->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
      key = update_tuple.KeyFromTuple(table_info_->schema_, p->key_schema_, p->index_->GetKeyAttrs());
      if (!p->index_->InsertEntry(key, r.value(), exec_ctx_->GetTransaction())) {
        return false;
      }
    }

    affect_cow++;
  }

  LOG_INFO("update end");

  std::vector<Value> values = {{TypeId::INTEGER, affect_cow}};
  Tuple output_tuple = Tuple{values, &GetOutputSchema()};
  *tuple = output_tuple;
  end_flag_ = true;
  return true;
}

}  // namespace bustub

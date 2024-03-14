//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "catalog/catalog.h"
#include "common/logger.h"
#include "execution/executors/delete_executor.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  end_flag_ = false;
}

void DeleteExecutor::Init() {
  LOG_INFO("delete  Executor Init");
  child_executor_->Init();
  end_flag_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  LOG_INFO("delete  Executor Next");
  if (end_flag_) {
    return false;
  }
  Catalog *cata_log = exec_ctx_->GetCatalog();
  TableInfo *table_info = cata_log->GetTable(plan_->TableOid());

  int affect_row = 0;
  while (child_executor_->Next(tuple, rid)) {
    //   std::cout<<"1tuple : "<<tuple<<std::endl;
    std::pair<TupleMeta, Tuple> res_tuple = table_info->table_->GetTuple(*rid);
    TupleMeta tuple_meta = res_tuple.first;
    tuple_meta.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(tuple_meta, *rid);

    // 2.更新索引
    std::vector<IndexInfo *> v = cata_log->GetTableIndexes(table_info->name_);

    for (auto p : v) {
      Tuple key =
          tuple->KeyFromTuple(table_info->schema_, p->key_schema_, p->index_->GetKeyAttrs());  // todo 不from 好像也行？
      p->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }

    // std::cout<<"2tuple : "<<tuple<<std::endl;
    // std::pair<TupleMeta, Tuple> &res_tuple = table_info->table_->GetTuple(*rid);
    // res_tuple.first.delete_txn_id_ = true;
    //  std::cout<<" is_deleted_ : "<<res_tuple.first.is_deleted_<<std::endl;
    affect_row++;
  }
  std::cout << " affect_row : " << affect_row << std::endl;
  std::vector<Value> values = {{TypeId::INTEGER, affect_row}};
  Tuple output_tuple = Tuple{values, &GetOutputSchema()};
  *tuple = output_tuple;
  end_flag_ = true;
  return true;
}

}  // namespace bustub

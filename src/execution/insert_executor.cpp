//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/config.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#define debug(x) std::cout << #x << " : " << (x) << std::endl;

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}
void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  child_executor_->Init();
  end_flag_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (end_flag_) {
    return false;
  }
  std::vector<Value> values;

  Tuple output_tuple;
  int32_t affect_cow = 0;
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->TableOid());
  auto tuple_meta = TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  // LOG_INFO("##Insert Next");
  while (child_executor_->Next(tuple, rid)) {
    // 1.像table 插入 tuple
    std::optional<RID> r = table_info->table_->InsertTuple(tuple_meta, *tuple, exec_ctx_->GetLockManager(),
                                                           exec_ctx_->GetTransaction(), plan_->TableOid());
    if (!r.has_value()) {
      return false;
    }

    //    debug(*rid);
    //    debug(r.value());

    *rid = r.value();
    LOG_DEBUG("insert tuple: %s", tuple->ToString(&child_executor_->GetOutputSchema()).c_str());
    LOG_DEBUG("rid of inserted tuple: %s", rid->ToString().c_str());
    // 2.更新索引
    std::vector<IndexInfo *> v = catalog->GetTableIndexes(table_info->name_);

    for (auto p : v) {
      Tuple key =
          tuple->KeyFromTuple(table_info->schema_, p->key_schema_, p->index_->GetKeyAttrs());  // todo 不from 好像也行？
      if (!p->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction())) {
        LOG_ERROR("index_->InsertEntry 插入后更新索引失败");
        return false;
      }
    }

    // 3. 返回的tuple 和插入的tuple不同，要包含一个整数 插入了多少行
    affect_cow++;
  }
  LOG_INFO("insert next's affect row %d", affect_cow);
  values = {{TypeId::INTEGER, affect_cow}};
  output_tuple = Tuple{values, &GetOutputSchema()};
  *tuple = output_tuple;
  end_flag_ = true;
  return true;
}

}  // namespace bustub

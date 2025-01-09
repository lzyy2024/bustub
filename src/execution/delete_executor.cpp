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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  has_inserted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple child_tuple{};
  RID child_rid{};

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    count++;
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);
    for (auto &index_info : indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      auto old_key = child_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);

      index->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> res = {{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &GetOutputSchema());
  return true;
}

}  // namespace bustub

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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (child_executor_->Next(tuple, rid)) {
    if (!table_info_->table_->MarkDelete(*rid, txn)) {
      return false;
    }

    exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);

    for (auto &index_info : indexes_) {
      index_info->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                          index_info->index_->GetKeyAttrs()),
                                      *rid, txn);
    }
    return true;
  }

  return false;
}

}  // namespace bustub

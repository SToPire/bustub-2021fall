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

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_->name_);

  if (plan_->IsRawInsert()) {
    total_size_ = plan_->RawValues().size();
    cur_size_ = 0;
  } else {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    do {
      if (cur_size_ == total_size_) {
        return false;
      }
      *tuple = Tuple(plan_->RawValuesAt(cur_size_++), &table_->schema_);
    } while (!table_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction()));
  } else {
    /* select insert */
    if (child_executor_->Next(tuple, rid)) {
      table_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    } else {
      return false;
    }
  }

  /* insert successfully, update index */
  for (auto &index_info : indexes_) {
    index_info->index_->InsertEntry(*tuple, *rid, exec_ctx_->GetTransaction());
  }

  return true;
}

}  // namespace bustub

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

#include "concurrency/transaction.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple new_tuple;
  Transaction *txn = exec_ctx_->GetTransaction();

  if (child_executor_->Next(tuple, rid)) {
    if (txn->IsSharedLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockUpgrade(txn, *rid);
    } else if (!txn->IsExclusiveLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);
    }

    new_tuple = GenerateUpdatedTuple(*tuple);
    if (!table_info_->table_->UpdateTuple(new_tuple, *rid, txn)) {
      return false;
    }

    for (auto &index_info : indexes_) {
      txn->AppendIndexWriteRecord(IndexWriteRecord(*rid, table_info_->oid_, WType::UPDATE, new_tuple,
                                                   index_info->index_oid_, exec_ctx_->GetCatalog(), *tuple));

      index_info->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                          index_info->index_->GetKeyAttrs()),
                                      *rid, txn);
      index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(), index_info->key_schema_,
                                 index_info->index_->GetKeyAttrs()),
          *rid, txn);
    }
    return true;
  }

  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub

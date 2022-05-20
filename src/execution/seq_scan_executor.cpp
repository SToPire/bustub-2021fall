//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cur_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() {
  TableInfo *table = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  cur_ = table->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (cur_ != end_) {
    *rid = cur_->GetRid();
    *tuple = *cur_++;
    if ((plan_->GetPredicate() == nullptr) ||
        plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      return true;
    }
  }

  return false;
}

}  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  us_.clear();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }

    DistinctKey key = MakeDistinctKey(tuple, child_executor_->GetOutputSchema());
    if (us_.find(key) == us_.end()) {
      us_.insert(key);
      return true;
    }
  }
}

}  // namespace bustub

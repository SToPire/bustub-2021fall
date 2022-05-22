//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  first_next_ = true;
  ht_.clear();

  column_cnt_ = plan_->OutputSchema()->GetColumnCount();
  for (uint32_t i = 0; i < column_cnt_; i++) {
    left_or_right_.push_back(
        static_cast<const ColumnValueExpression *>(plan_->OutputSchema()->GetColumn(i).GetExpr())->GetTupleIdx());
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  /* enter upon first Next() invocation */
  if (first_next_) {
    while (left_executor_->Next(tuple, rid)) {
      HashJoinKey left_key = GetLeftJoinKey(tuple);
      if (ht_.find(left_key) == ht_.end()) {
        ht_.insert({left_key, std::vector<Tuple>()});
      }
      ht_[left_key].push_back(*tuple);
    }

    while (right_executor_->Next(tuple, rid)) {
      HashJoinKey right_key = GetRightJoinKey(tuple);
      if (ht_.find(right_key) != ht_.end()) {
        for (Tuple &left_tuple : ht_[right_key]) {
          std::vector<Value> values;
          for (uint32_t i = 0; i < column_cnt_; i++) {
            values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(
                left_or_right_[i] == 0 ? &left_tuple : tuple,
                left_or_right_[i] == 0 ? plan_->GetLeftPlan()->OutputSchema() : plan_->GetRightPlan()->OutputSchema()));
          }
          res_.emplace_back(Tuple(values, plan_->OutputSchema()));
        }
      }
    }

    first_next_ = false;
    res_iterator_ = res_.begin();
  }

  if (res_iterator_ == res_.end()) {
    return false;
  }
  *tuple = *res_iterator_++;
  return true;
}

}  // namespace bustub

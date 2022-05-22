//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  first_next_ = true;
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (first_next_) {
    while (child_->Next(tuple, rid)) {
      aht_.InsertCombine(MakeAggregateKey(tuple), MakeAggregateValue(tuple));
    }
    first_next_ = false;
    aht_iterator_ = aht_.Begin();
  }

  while (aht_iterator_ != aht_.End()) {
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()
            ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
            .GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &output_column : plan_->OutputSchema()->GetColumns()) {
        values.push_back(output_column.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_,
                                                                    aht_iterator_.Val().aggregates_));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }

  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub

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
  Tuple cur_tuple;
  RID cur_rid;
  std::vector<AbstractExpressionRef> group_by = plan_->GetGroupBys();
  std::vector<AbstractExpressionRef> aggregates = plan_->GetAggregates();
  while (child_->Next(&cur_tuple, &cur_rid)) {
    AggregateKey agg_key;
    AggregateValue agg_val;
    for (auto &exp : group_by) {
      agg_key.group_bys_.emplace_back(exp->Evaluate(&cur_tuple, child_->GetOutputSchema()));
    }
    for (auto &exp : aggregates) {
      agg_val.aggregates_.emplace_back(exp->Evaluate(&cur_tuple, child_->GetOutputSchema()));
    }
    aht_.InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }
    AggregateValue val;
    if (aht_.IsEmpty(&val)) {
      *tuple = Tuple(val.aggregates_, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
    return false;
  }
  AggregateKey key = aht_iterator_.Key();
  AggregateValue val = aht_iterator_.Val();
  std::vector<Value> item{};
  for (const auto &k : key.group_bys_) {
    item.emplace_back(k);
  }
  for (const auto &v : val.aggregates_) {
    item.emplace_back(v);
  }
  *tuple = Tuple(item, &GetOutputSchema());
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

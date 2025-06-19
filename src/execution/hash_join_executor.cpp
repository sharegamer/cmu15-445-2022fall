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
#include "type/value_factory.h"
// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;

  while (right_child_->Next(&tuple, &rid)) {
    Value key = plan_->RightJoinKeyExpression().Evaluate(&tuple, right_child_->GetOutputSchema());
    hash_table_[key].emplace_back(tuple);
  }

  while (left_child_->Next(&tuple, &rid)) {
    Value key = plan_->LeftJoinKeyExpression().Evaluate(&tuple, left_child_->GetOutputSchema());
    auto iter = hash_table_.find(key);

    if (iter != hash_table_.end()) {
      for (const auto &rtuple : iter->second) {
        std::vector<Value> values;

        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(rtuple.GetValue(&right_child_->GetOutputSchema(), i));
        }
        result_tuples_.emplace_back(Tuple(values, &plan_->OutputSchema()));
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      result_tuples_.emplace_back(Tuple(values, &plan_->OutputSchema()));
    }
  }
}
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_tuples_.empty()) {
    return false;
  }
  *tuple = result_tuples_.front();
  result_tuples_.erase(result_tuples_.begin());
  return true;
}
}  // namespace bustub

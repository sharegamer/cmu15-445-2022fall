//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple l_tuple;
  Tuple r_tuple;
  RID lrid;
  RID rrid;
  const Schema &left_schema = left_executor_->GetOutputSchema();
  const Schema &right_schema = right_executor_->GetOutputSchema();
  const Schema &output_schema = plan_->OutputSchema();
  std::vector<Tuple> ltuples;
  std::vector<Tuple> rtuples;
  while (left_executor_->Next(&l_tuple, &lrid)) {
    ltuples.emplace_back(l_tuple);
  }
  while (right_executor_->Next(&r_tuple, &rrid)) {
    rtuples.emplace_back(r_tuple);
  }

  for (auto &left_tuple : ltuples) {
    bool find_match = false;
    for (auto &right_tuple : rtuples) {
      Value value = plan_->predicate_->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (!value.IsNull() && value.GetAs<bool>()) {
        find_match = true;
        std::vector<Value> values;

        for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
          values.push_back(left_tuple.GetValue(&left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
          values.push_back(right_tuple.GetValue(&right_schema, i));
        }
        Tuple tuple = Tuple(values, &output_schema);
        results_.emplace(tuple);
      }
    }
    if (!find_match && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;

      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }
      for (const auto &col : right_schema.GetColumns()) {
        values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      Tuple tuple = Tuple(values, &output_schema);
      results_.emplace(tuple);
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (results_.empty()) {
    return false;
  }
  *tuple = results_.front();
  *rid = tuple->GetRid();
  results_.pop();
  return true;
}

}  // namespace bustub

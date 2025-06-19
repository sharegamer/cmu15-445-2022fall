//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_;
  std::vector<Tuple> sorted_tuples_;
};

class TupleComparator {
 public:
  TupleComparator(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys, const Schema &schema)
      : order_bys_(order_bys), schema_(schema) {}

  auto operator()(const Tuple &a, const Tuple &b) const -> bool {
    for (const auto &[order_type, expr] : order_bys_) {
      Value a_val = expr->Evaluate(&a, schema_);
      Value b_val = expr->Evaluate(&b, schema_);

      if (a_val.CompareEquals(b_val) == CmpBool::CmpTrue) {
        continue;
      }

      if (order_type == OrderByType::DESC) {
        return a_val.CompareGreaterThan(b_val) == CmpBool::CmpTrue;
      }

      return a_val.CompareLessThan(b_val) == CmpBool::CmpTrue;
    }

    return false;
  }

 private:
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys_;
  const Schema &schema_;
};
}  // namespace bustub

#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      min_heap_(TupleComparator(plan->GetOrderBy(), child_executor_->GetOutputSchema())) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    min_heap_.push(tuple);
    if (min_heap_.size() > plan_->GetN()) {
      min_heap_.pop();
    }
  }
  while (!min_heap_.empty()) {
    result_.push_back(min_heap_.top());
    min_heap_.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_.empty()) {
    return false;
  }
  *tuple = result_.back();
  result_.pop_back();
  return true;
}
}  // namespace bustub

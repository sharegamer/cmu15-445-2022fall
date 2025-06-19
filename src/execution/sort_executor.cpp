#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), TupleComparator(plan_->GetOrderBy(), plan_->OutputSchema()));
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuples_.empty()) {
    return false;
  }
  *tuple = sorted_tuples_.front();
  sorted_tuples_.erase(sorted_tuples_.begin());
  return true;
}

}  // namespace bustub

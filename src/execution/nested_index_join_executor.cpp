//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_->Init();
  Tuple ltuple;
  RID lrid;
  const auto &predicate = plan_->KeyPredicate();
  const auto &left_schema = child_->GetOutputSchema();
  const auto &right_schema = plan_->InnerTableSchema();
  const auto &output_schema = plan_->OutputSchema();
  index_oid_t index_id = plan_->GetIndexOid();
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(index_id);
  Index *index = index_info->index_.get();

  TableHeap *table_heap = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
  while (child_->Next(&ltuple, &lrid)) {
    Value value = predicate->Evaluate(&ltuple, child_->GetOutputSchema());
    std::vector<RID> rids;
    Tuple key_value = Tuple(std::vector<Value>{value}, &index_info->key_schema_);

    index->ScanKey(key_value, &rids, exec_ctx_->GetTransaction());

    for (RID rid : rids) {
      Tuple rtuple;
      table_heap->GetTuple(rid, &rtuple, exec_ctx_->GetTransaction());

      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(ltuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(rtuple.GetValue(&right_schema, i));
      }
      Tuple tuple(values, &output_schema);
      results_.emplace(tuple);
    }

    if (plan_->GetJoinType() == JoinType::LEFT && rids.empty()) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(ltuple.GetValue(&left_schema, i));
      }
      for (const auto &col : right_schema.GetColumns()) {
        values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      Tuple tuple = Tuple(values, &output_schema);
      results_.emplace(tuple);
    }
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (results_.empty()) {
    return false;
  }
  *tuple = results_.front();
  *rid = tuple->GetRid();
  results_.pop();
  return true;
}

}  // namespace bustub

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
#include "execution/executors/aggregation_executor.h"
#include <common/logger.h>
#include <memory>
#include <vector>

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    // Insert the key and value into the aggregation hash table
    aht_->InsertCombine(key, value);
  }
  // Initialize the aggregation hash table iterator
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
  has_inserted_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("Start AggregationExecutor Next");
  // test

  if (aht_->Begin() != aht_->End()) {
    if (*aht_iterator_ == aht_->End()) {
      return false;
    }

    auto key = aht_iterator_->Key();
    auto value = aht_iterator_->Val();

    AggregateValue values{};
    values.aggregates_.reserve(key.group_bys_.size() + value.aggregates_.size());
    // LOG_INFO("Key size: %lu, value size: %lu", key.group_bys_.size(), value.aggregates_.size());
    for (auto &key_val : key.group_bys_) {
      values.aggregates_.emplace_back(key_val);
    }
    for (auto &value_val : value.aggregates_) {
      values.aggregates_.emplace_back(value_val);
    }

    // for (auto &val : values.aggregates_) {
    // 	LOG_INFO("Value: %s", val.ToString().c_str());
    // }

    *tuple = Tuple(values.aggregates_, &GetOutputSchema());
    ++(*aht_iterator_);
    return true;
  }
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;

  if (plan_->group_bys_.empty()) {
    auto value = aht_->GenerateInitialAggregateValue();
    *tuple = Tuple(value.aggregates_, &GetOutputSchema());
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub

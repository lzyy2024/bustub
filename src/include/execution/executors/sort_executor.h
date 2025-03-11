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
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/index/generic_key.h"
#include "storage/table/tuple.h"

namespace bustub {

class Comparator {
 public:
  Comparator(const Schema *schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
      : schema_(schema), order_bys_(std::move(order_bys)) {}
  auto operator()(const Tuple &a, const Tuple &b) const -> bool {
    for (const auto &order_by : order_bys_) {
      const auto &expr = order_by.second;
      const auto &a_val = expr->Evaluate(&a, *schema_);
      const auto &b_val = expr->Evaluate(&b, *schema_);
      if (a_val.CompareEquals(b_val) == CmpBool::CmpTrue) {
        continue;
      }
      if (a_val.CompareLessThan(b_val) == CmpBool::CmpTrue) {
        return order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT;
      }
      return order_by.first == OrderByType::DESC;
    }
    return false;
  }

 private:
  const Schema *schema_;
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
};

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
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> tuples_;
  std::vector<Tuple>::iterator iter_;
};
}  // namespace bustub

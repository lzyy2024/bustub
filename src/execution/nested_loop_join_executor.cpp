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
#include "common/logger.h"
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
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  has_inserted_ = false;
  left_executor_->Next(&left_tuple_, &left_rid_);
  has_done_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //   LOG_INFO("start NextedLoopJoinExectuor: \n");
  auto predicate = plan_->Predicate();
  Tuple right_tuple;
  RID right_rid;

  if (plan_->GetJoinType() == JoinType::LEFT) {
    do {
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        if (predicate
                ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                               right_executor_->GetOutputSchema())
                .GetAs<bool>()) {
          std::vector<Value> values;
          for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(right_tuple.GetValue(&this->right_executor_->GetOutputSchema(), i));
          }
          *tuple = Tuple(values, &GetOutputSchema());
          has_done_ = true;
          return true;
        }
      }
      if (!has_done_) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(this->right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        has_done_ = true;
        return true;
      }
      right_executor_->Init();
      has_done_ = false;
    } while (left_executor_->Next(&left_tuple_, &left_rid_));
    return false;
  }
  do {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto join_result = predicate
                             ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                            right_executor_->GetOutputSchema())
                             .GetAs<bool>();
      if (join_result) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&this->right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        has_done_ = true;
        return true;
      }
    }
    right_executor_->Init();
    has_done_ = false;
    //   return false;
  } while (left_executor_->Next(&left_tuple_, &left_rid_));
  return false;
}
}  // namespace bustub

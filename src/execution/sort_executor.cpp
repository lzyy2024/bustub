#include "execution/executors/sort_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  iter_ = tuples_.begin();
  auto order_bys = plan_->GetOrderBy();
  std::sort(tuples_.begin(), tuples_.end(), Comparator(&child_executor_->GetOutputSchema(), order_bys));
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != tuples_.end()) {
    *tuple = *iter_;
    iter_++;
    return true;
  }
  return false;
}
}  // namespace bustub

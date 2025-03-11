#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  std::priority_queue<Tuple, std::vector<Tuple>, HeapComparator> heap(
      HeapComparator(&this->GetOutputSchema(), plan_->GetOrderBy()));
  size_t heapmax_size = plan_->GetN();
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    heap.emplace(tuple);
    if (heap.size() > heapmax_size) {
      heap.pop();
    }
  }
  while (!heap.empty()) {
    heap_stack_.push(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!heap_stack_.empty()) {
    *tuple = heap_stack_.top();
    heap_stack_.pop();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_stack_.size(); };

}  // namespace bustub

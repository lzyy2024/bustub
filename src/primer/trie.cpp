#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto tr = Trie::GetRoot();
  if (tr == nullptr) {
    return nullptr;
  }

  for (auto &i : key) {
    auto it = tr->children_.find(i);
    if (it != tr->children_.end()) {
      tr = it->second;
    } else {
      return nullptr;
    }
  }

  auto ret = dynamic_cast<const TrieNodeWithValue<T> *>(tr.get());
  if (ret == nullptr) {
    return nullptr;
  }
  return ret->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  std::shared_ptr<TrieNode> new_root;
  if (root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = std::shared_ptr<TrieNode>(Trie::GetRoot()->Clone());
  }
  auto cur = new_root;
  if (key.empty()) {
    cur = std::make_shared<TrieNodeWithValue<T>>(cur->children_, std::make_shared<T>(std::move(value)));
    return Trie(cur);
  }
  int n = key.size();
  for (int i = 0; i < n - 1; i++) {
    if (cur->children_.find(key[i]) == cur->children_.end()) {
      cur->children_[key[i]] = std::make_shared<TrieNode>();
    } else {
      cur->children_[key[i]] = cur->children_.at(key[i])->Clone();
    }
    auto cur1 = std::const_pointer_cast<TrieNode>(cur->children_[key[i]]);
    cur = std::dynamic_pointer_cast<TrieNode>(cur1);
  }
  if (cur->children_.find(key.back()) == cur->children_.end()) {
    cur->children_[key.back()] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    cur->children_[key.back()] = std::make_shared<TrieNodeWithValue<T>>(cur->children_[key.back()]->children_,
                                                                        std::make_shared<T>(std::move(value)));
  }
  return Trie(new_root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::shared_ptr<TrieNode> new_root = Trie::GetRoot()->Clone();
  auto cur = new_root;

  if (key.empty()) {
    if (cur->is_value_node_) {
      std::shared_ptr<TrieNode> trie_node = std::make_shared<TrieNode>(cur->children_);
      cur = trie_node;
    }
    return Trie(cur);
  }

  std::stack<std::shared_ptr<TrieNode>> st;
  std::shared_ptr<TrieNode> pre;
  int n = key.size();
  for (int i = 0; i < n; i++) {
    st.push(cur);
    if (cur->children_.find(key[i]) == cur->children_.end()) {
      return Trie(new_root);
    }
    auto clone = cur->children_.find(key[i])->second->Clone();
    if (cur == nullptr || clone == nullptr) {
      return Trie(new_root);
    }
    cur->children_[key[i]] = std::shared_ptr<const TrieNode>(std::move(clone));
    pre = cur;
    cur = std::const_pointer_cast<TrieNode>(cur->children_[key[i]]);
  }

  if (cur->is_value_node_) {
    pre->children_[key.back()] = std::make_shared<TrieNode>(cur->Clone()->children_);
  }

  int idx = key.size() - 1;
  while (!st.empty()) {
    std::shared_ptr<TrieNode> node = st.top();
    if (node->children_[key[idx]]->is_value_node_) {
      break;
    }
    if (node->children_[key[idx]]->children_.empty()) {
      node->children_.erase(node->children_.find(key[idx--]));
    } else {
      break;
    }
    st.pop();
  }
  if (new_root->children_.empty() && !new_root->is_value_node_) {
    new_root = nullptr;
  }
  return Trie(new_root);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
#include "primer/trie.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"
#include "execution/executors/topn_executor.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> now = root_;
  for (auto c : key) {
    auto it = now->children_.find(c);
    if (it == now->children_.end()) {
      return nullptr;
    }
    std::shared_ptr<const TrieNode> temp = it->second;
    now = temp;
  }
  // auto valuenode = dynamic_cast<const TrieNodeWithValue<T>*>(now);
  auto value_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(now);
  if (value_node == nullptr) {
    return nullptr;
  }
  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  Trie t;
  std::shared_ptr<TrieNode> now;
  if (root_ == nullptr) {
    auto new_root = std::shared_ptr<TrieNode>();
    t.root_ = new_root;
    now = new_root;
  } else {
    auto un = root_->Clone();
    auto new_root = std::shared_ptr<TrieNode>(std::move(un));

    t.root_ = new_root;
    now = new_root;
  }

  for (char c : key) {
    if (now->children_.find(c) == now->children_.end()) {
      std::shared_ptr<TrieNode> temp = std::make_shared<TrieNode>();
      now->children_[c] = temp;
      now = temp;
    } else {
      auto tempp = now->children_[c]->Clone();
      std::shared_ptr<TrieNode> temp = std::shared_ptr<TrieNode>(std::move(tempp));
      now->children_[c] = temp;
      now = temp;
    }
  }

  // std::shared_ptr<TrieNode> now2 = std::make_shared<TrieNode>(std::move(now));
  std::shared_ptr<TrieNodeWithValue<T>> value_node = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(now);
  std::shared_ptr<T> v = std::make_shared<T>(std::move(value));
  value_node->value_ = std::move(v);
  value_node->is_value_node_ = true;
  return t;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  Trie t;
  std::shared_ptr<TrieNode> new_root = std::make_shared<TrieNode>();
  if (root_ != nullptr) {
    auto un = root_->Clone();
    new_root = std::shared_ptr<TrieNode>(std::move(un));
    t.root_ = new_root;
  } else {
    t.root_ = new_root;
  }
  std::shared_ptr<TrieNode> now = new_root;
  auto pa = now;
  for (auto c : key) {
    if (now->children_.find(c) == now->children_.end()) {
      return t;
    }
    auto node = now->children_.at(c)->Clone();
    auto node2 = std::shared_ptr<TrieNode>(std::move(node));

    pa = now;
    now = node2;
  }
  if (now->is_value_node_ && now->children_.empty()) {
    pa->children_.erase(key.back());
  }
  return t;
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

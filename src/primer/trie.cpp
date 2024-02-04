#include "primer/trie.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"
#include "execution/executors/topn_executor.h"

#define debug(x) std::cout << #x << ":" << x << '\n';

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
  std::shared_ptr<const TrieNode> now = std::make_shared<const TrieNode>();
  now = root_;
  for (auto c : key) {
    auto it = now->children_.find(c);
    if (it == now->children_.end()) {
      return nullptr;
    }
    now = now->children_.at(c);
  }

  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(now.get());
  if (value_node == nullptr || !value_node->is_value_node_) {
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
  std::shared_ptr<TrieNode> now = std::make_shared<TrieNode>();
  std::shared_ptr<TrieNode> pa = std::make_shared<TrieNode>();

  if (root_ == nullptr) {
    std::shared_ptr<TrieNode> new_root = std::make_shared<TrieNode>();
    now = new_root;
    t.root_ = new_root;

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
      pa = now;
      now = temp;
    } else {
      auto tempp = now->children_[c]->Clone();
      std::shared_ptr<TrieNode> temp = std::shared_ptr<TrieNode>(std::move(tempp));
      now->children_[c] = temp;
      pa = now;
      now = temp;
    }
  }

  if (!key.empty()) {
    std::shared_ptr<T> v = std::make_shared<T>(std::move(value));
    std::shared_ptr<TrieNode> value_node =
        std::make_shared<TrieNodeWithValue<T>>(std::move(now->children_), std::move(v));
    pa->children_[key.back()] = value_node;
  } else {
    std::shared_ptr<T> v = std::make_shared<T>(std::move(value));
    t.root_ = std::make_shared<TrieNodeWithValue<T>>(std::move(now->children_), std::move(v));
  }
  return t;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  Trie t;
  auto node = root_->Clone();
  // if (!root_->is_value_node_) {
  //   node->is_value_node_ = false;
  // }
  debug(root_->is_value_node_);
  debug(node->is_value_node_);
  debug(key);
  debug(&(*root_));
  debug(&(*node));
  auto new_root = std::shared_ptr<TrieNode>(std::move(node));
  auto now = new_root;

  std::vector<std::shared_ptr<TrieNode>> v;

  for (auto c : key) {
    if (now->children_.find(c) == now->children_.end()) {
      t.root_ = now;
      return t;
    }
    v.push_back(now);
    auto node = now->children_[c]->Clone();
    now = std::shared_ptr<TrieNode>(std::move(node));
  }

  v.push_back(now);

  if (v.back()->children_.empty()) {
    auto old_node = v.back();
    v.pop_back();
    auto new_node = old_node->Clone();
    auto new_node2 = std::shared_ptr<TrieNode>(std::move(new_node));
    new_node2->is_value_node_ = false;
    v.push_back(new_node2);

    int sz = v.size();
    int sz_str = key.size();
    int flag = 1;
    for (int i = sz - 1, j = sz_str - 1; i >= 1 && j >= 0; i--, j--) {
      auto node_i = v[i];
      auto node_j = v[i - 1];
      // if (key == "test") {
      //   debug(node_i->children_.empty());
      //   debug(node_i->is_value_node_);
      // }

      if (node_i->children_.empty() && !node_i->is_value_node_ && (flag != 0)) {
        node_j->children_.erase(key[j]);
        continue;
      }
      flag = 0;
      node_j->children_[key[j]] = node_i;
    }
  } else {
    auto old_node = v.back();
    v.pop_back();

    auto new_node = old_node->Clone();
    auto new_node2 = std::shared_ptr<TrieNode>(std::move(new_node));
    auto new_node3 = std::make_shared<TrieNode>(new_node2->children_);
    // new_node2 = static_cast<std::shared_ptr<TrieNode>>(new_node2);
    // new_node2->is_value_node_ = false;  // todo:为什么值节点改不成功

    v.push_back(new_node3);
    int sz = v.size();
    int sz_str = key.size();
    for (int i = sz - 1, j = sz_str - 1; i >= 1 && j >= 0; i--, j--) {
      auto node_i = v[i];
      auto node_j = v[i - 1];
      node_j->children_[key[j]] = node_i;
      if (i == 1) {
        // debug(&(*root_));
        // debug(&(*node_j));
      }
    }
  }

  if (v[0]->children_.empty()) {
    t.root_ = nullptr;
    return t;
  }
  debug(&(*v[0]));
  debug(v[0]->is_value_node_);
  t.root_ = v[0];

  return t;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked
// up by the linker.

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

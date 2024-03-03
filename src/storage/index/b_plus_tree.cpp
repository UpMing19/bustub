#include "storage/index/b_plus_tree.h"
#include <cstddef>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

#define debug(x) std::cout << #x << " : " << (x) << std::endl;

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  // LOG_INFO("缓存池大小: pool size = %zu", (size_t)bpm_->GetPoolSize());
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;
  (void)ctx;
  LOG_INFO("Get key : %s", std::to_string(key.ToString()).c_str());
  if (GetRootPageId() == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_node = head_guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = head_node->root_page_id_;

  ReadPageGuard guard = bpm_->FetchPageRead(ctx.root_page_id_);

  page_id_t next_page_id;
  auto tree_page = guard.As<BPlusTreePage>();

  while (!tree_page->IsLeafPage()) {
    auto internal_node = guard.As<InternalPage>();
    internal_node->FindValue(key, next_page_id, comparator_);
    guard = bpm_->FetchPageRead(next_page_id);
    tree_page = guard.As<BPlusTreePage>();
  }
  auto leaf_node = guard.As<LeafPage>();
  ctx.read_set_.push_back(std::move(guard));

  ValueType value;
  int index = leaf_node->FindValue(key, value, comparator_);
  if (index != -1 && (comparator_(leaf_node->KeyAt(index), key) == 0)) {
    value = leaf_node->ValueAt(index);
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::HappyInsert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_node = head_guard.As<BPlusTreeHeaderPage>();

  if (head_node->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard guard = bpm_->FetchPageRead(head_node->root_page_id_);

  page_id_t next_page_id;
  auto tree_page = guard.As<BPlusTreePage>();

  while (!tree_page->IsLeafPage()) {
    auto internal_node = guard.As<InternalPage>();
    internal_node->FindValue(key, next_page_id, comparator_);
    guard = bpm_->FetchPageRead(next_page_id);
    tree_page = guard.As<BPlusTreePage>();
  }
  if (!tree_page->IsInsertSafe()) {
    return false;
  }
  page_id_t leaf_page_id = guard.PageId();
  guard.Drop();
  WritePageGuard lead_page_guard = bpm_->FetchPageWrite(leaf_page_id);
  auto node = lead_page_guard.AsMut<LeafPage>();
  ValueType v;
  int index = node->FindValue(key, v, comparator_);
  if (index != -1 && comparator_(key, node->KeyAt(index)) == 0) {
    return false;
  }
  if (index == -1) {
    index = node->GetSize();
  }
  node->IncreaseSize(1);
  for (int i = node->GetSize() - 1; i > index; i--) {
    node->SetKeyAt(i, node->KeyAt(i - 1));
    node->SetValueAt(i, node->ValueAt(i - 1));
  }
  node->SetKeyAt(index, key);
  node->SetValueAt(index, value);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // std::cout << "Thread ID: " << thread << " -- Find | key : " << std::to_string(key.ToString()).c_str() << std::endl;
  Context ctx;
  (void)ctx;
  LOG_INFO("Insert key : %s", std::to_string(key.ToString()).c_str());
  // if (HappyInsert(key, value, txn)) {
  //   return true;
  // }

  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();

  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    BasicPageGuard guard = bpm_->NewPageGuarded(&head_page->root_page_id_);
    WritePageGuard guard1 = bpm_->FetchPageWrite(head_page->root_page_id_);
    auto leaf_node = guard1.AsMut<LeafPage>();
    ctx.root_page_id_ = head_page->root_page_id_;
    leaf_node->Init(leaf_max_size_);
    leaf_node->IncreaseSize(1);
    leaf_node->SetKeyAt(0, key);
    leaf_node->SetValueAt(0, value);
    return true;
  }
  ctx.root_page_id_ = head_page->root_page_id_;

  ctx.write_set_.push_back(std::move(ctx.header_page_.value()));
  ctx.header_page_ = std::nullopt;

  WritePageGuard guard = bpm_->FetchPageWrite(head_page->root_page_id_);
  auto tree_node = guard.AsMut<BPlusTreePage>();

  while (!tree_node->IsLeafPage()) {
    auto internal_node = guard.AsMut<InternalPage>();
    page_id_t v;
    internal_node->FindValue(key, v, comparator_);
    ctx.write_set_.push_back(std::move(guard));
    guard = bpm_->FetchPageWrite(v);
    tree_node = guard.AsMut<BPlusTreePage>();
  }

  auto leaf_node = guard.AsMut<LeafPage>();
  ctx.write_set_.push_back(std::move(guard));
  InsertLeafNode(leaf_node, key, value, ctx, txn);
  // LOG_INFO("Insert key : %s", std::to_string(key.ToString()).c_str());
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeafNode(LeafPage *node, const KeyType &key, const ValueType &value, Context &ctx,
                                    Transaction *txn) -> void {
  ValueType v;
  int index = node->FindValue(key, v, comparator_);
  if (index != -1 && comparator_(node->KeyAt(index), key) == 0) {
    // LOG_INFO(" key重复 ");
    return;
  }
  if (node->GetSize() + 1 == node->GetMaxSize()) {
    SplitLeafNode(node, key, value, ctx, txn);
  } else {
    index = node->FindValue(key, v, comparator_);
    if (index == -1) {
      index = node->GetSize();
    }
    node->IncreaseSize(1);
    for (int i = node->GetSize() - 1; i > index; i--) {
      node->SetKeyAt(i, node->KeyAt(i - 1));
      node->SetValueAt(i, node->ValueAt(i - 1));
    }
    node->SetKeyAt(index, key);
    node->SetValueAt(index, value);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage *node, const KeyType &key, const ValueType &value, Context &ctx,
                                   Transaction *txn) -> void {
  page_id_t pid;
  BasicPageGuard guard = bpm_->NewPageGuarded(&pid);
  auto new_leaf_node = guard.AsMut<LeafPage>();
  new_leaf_node->Init(leaf_max_size_);
  bool put_left = false;

  for (int i = node->GetMinSize() - 1; i < node->GetMinSize(); i++) {
    if (comparator_(key, node->KeyAt(i)) < 0) {
      put_left = true;
      break;
    }
  }
  if (put_left) {
    int mid = node->GetMinSize() - 1;
    int num = 0;
    for (int i = mid, j = 0; i < node->GetSize(); j++, i++) {
      new_leaf_node->SetKeyAt(j, node->KeyAt(i));
      new_leaf_node->SetValueAt(j, node->ValueAt(i));
      num++;
    }
    new_leaf_node->IncreaseSize(num);
    node->IncreaseSize(-num);
    // InsertLeafNode(node, key, value, ctx, txn);
    ValueType v;
    int index = node->FindValue(key, v, comparator_);
    if (index == -1) {
      index = node->GetSize();
    }
    node->IncreaseSize(1);
    for (int i = node->GetSize() - 1; i > index; i--) {
      node->SetKeyAt(i, node->KeyAt(i - 1));
      node->SetValueAt(i, node->ValueAt(i - 1));
    }
    node->SetKeyAt(index, key);
    node->SetValueAt(index, value);

  } else {
    int mid = node->GetMinSize() - 1;
    int num = 0;
    for (int i = mid + 1, j = 0; i < node->GetSize(); j++, i++) {
      new_leaf_node->SetKeyAt(j, node->KeyAt(i));
      new_leaf_node->SetValueAt(j, node->ValueAt(i));
      num++;
    }
    new_leaf_node->IncreaseSize(num);
    node->IncreaseSize(-num);

    ValueType v;
    int index = new_leaf_node->FindValue(key, v, comparator_);
    if (index == -1) {
      index = new_leaf_node->GetSize();
    }
    new_leaf_node->IncreaseSize(1);
    for (int i = new_leaf_node->GetSize() - 1; i > index; i--) {
      new_leaf_node->SetKeyAt(i, new_leaf_node->KeyAt(i - 1));
      new_leaf_node->SetValueAt(i, new_leaf_node->ValueAt(i - 1));
    }
    new_leaf_node->SetKeyAt(index, key);
    new_leaf_node->SetValueAt(index, value);
  }

  page_id_t nxt = node->GetNextPageId();
  new_leaf_node->SetNextPageId(nxt);
  node->SetNextPageId(pid);

  InsertParent(new_leaf_node->KeyAt(0), pid, ctx, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalNode(InternalPage *node, const KeyType &key, const page_id_t &value, Context &ctx,
                                       Transaction *txn) -> void {
  // 1.找up_key
  KeyType up_key{};
  page_id_t up_key_value;

  int mid = node->GetMinSize() - 1;
  bool put_left = false;
  if (comparator_(key, node->KeyAt(mid)) < 0) {
    put_left = true;
    up_key = node->KeyAt(mid);
    up_key_value = node->ValueAt(mid);
  } else {
    if (comparator_(key, node->KeyAt(mid + 1)) > 0) {
      up_key = node->KeyAt(mid + 1);
      up_key_value = node->ValueAt(mid + 1);
    } else {
      up_key = key;
      up_key_value = value;
    }
    mid++;
  }

  // 2.删除up_key

  if (comparator_(up_key, key) != 0) {
    for (int i = mid; i < node->GetSize() - 1; i++) {
      node->SetKeyAt(i, node->KeyAt(i + 1));
      node->SetValueAt(i, node->ValueAt(i + 1));
    }
    node->IncreaseSize(-1);
  }

  // 3.创建一个新node，并移动一半数据过去
  page_id_t pid;
  BasicPageGuard guard = bpm_->NewPageGuarded(&pid);
  auto new_internal_node = guard.AsMut<InternalPage>();
  new_internal_node->Init(internal_max_size_);
  new_internal_node->IncreaseSize(1);
  int num = 0;
  for (int i = mid, j = 1; i < node->GetSize(); i++, j++) {
    new_internal_node->SetKeyAt(j, node->KeyAt(i));
    new_internal_node->SetValueAt(j, node->ValueAt(i));
    num++;
  }
  new_internal_node->IncreaseSize(num);
  node->IncreaseSize(-num);
  // 4.插入key到 左边或者右边 或者不插 直接作为up_key传上去
  if (comparator_(key, up_key) != 0) {
    int index = -1;
    auto internal_node = node;
    if (!put_left) {
      internal_node = new_internal_node;
    }
    page_id_t v;
    index = internal_node->FindValue(key, v, comparator_);
    index++;
    internal_node->IncreaseSize(1);
    for (int i = internal_node->GetSize() - 1; i > index; i--) {
      internal_node->SetKeyAt(i, internal_node->KeyAt(i - 1));
      internal_node->SetValueAt(i, internal_node->ValueAt(i - 1));
    }
    internal_node->SetKeyAt(index, key);
    internal_node->SetValueAt(index, value);

    new_internal_node->SetKeyAt(0, KeyType{});
    new_internal_node->SetValueAt(0, up_key_value);
  } else {
    new_internal_node->SetKeyAt(0, KeyType{});
    new_internal_node->SetValueAt(0, up_key_value);
  }

  InsertParent(up_key, pid, ctx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertParent(const KeyType &key, const page_id_t &value, Context &ctx, Transaction *txn) -> void {
  page_id_t cur_page_id = ctx.write_set_.back().PageId();
  if (ctx.IsRootPage(cur_page_id)) {
    page_id_t old_root_page_id = ctx.root_page_id_;

    WritePageGuard &head_page_guard = ctx.write_set_.front();
    auto head_page = head_page_guard.AsMut<BPlusTreeHeaderPage>();

    BasicPageGuard guard = bpm_->NewPageGuarded(&head_page->root_page_id_);
    ctx.root_page_id_ = head_page->root_page_id_;
    guard.Drop();

    WritePageGuard new_root_guard = bpm_->FetchPageWrite(head_page->root_page_id_);
    auto new_root_node = new_root_guard.AsMut<InternalPage>();

    new_root_node->Init(internal_max_size_);
    new_root_node->IncreaseSize(1);
    new_root_node->IncreaseSize(1);
    new_root_node->SetKeyAt(1, key);
    new_root_node->SetValueAt(1, value);

    new_root_node->SetKeyAt(0, KeyType{});
    new_root_node->SetValueAt(0, old_root_page_id);
    ctx.write_set_.clear();
    return;
  }
  ctx.write_set_.pop_back();
  WritePageGuard &guard = ctx.write_set_.back();

  auto internal_node = guard.AsMut<InternalPage>();

  if (internal_node->GetSize() == internal_node->GetMaxSize()) {
    SplitInternalNode(internal_node, key, value, ctx, txn);
  } else {
    int index = -1;
    page_id_t v;
    index = internal_node->FindValue(key, v, comparator_);
    index++;
    internal_node->IncreaseSize(1);
    for (int i = internal_node->GetSize() - 1; i > index; i--) {
      internal_node->SetKeyAt(i, internal_node->KeyAt(i - 1));
      internal_node->SetValueAt(i, internal_node->ValueAt(i - 1));
    }
    internal_node->SetKeyAt(index, key);
    internal_node->SetValueAt(index, value);
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::HappyRemove(const KeyType &key, Transaction *txn) -> int {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_node = head_guard.As<BPlusTreeHeaderPage>();

  if (head_node->root_page_id_ == INVALID_PAGE_ID) {
    return 1;
  }
  ReadPageGuard guard = bpm_->FetchPageRead(head_node->root_page_id_);

  page_id_t next_page_id;
  auto tree_page = guard.As<BPlusTreePage>();

  while (!tree_page->IsLeafPage()) {
    auto internal_node = guard.As<InternalPage>();
    internal_node->FindValue(key, next_page_id, comparator_);
    guard = bpm_->FetchPageRead(next_page_id);
    tree_page = guard.As<BPlusTreePage>();
  }

  page_id_t leaf_page_id = guard.PageId();
  guard.Drop();
  WritePageGuard lead_page_guard = bpm_->FetchPageWrite(leaf_page_id);
  auto node = lead_page_guard.AsMut<LeafPage>();

  int index = -1;
  ValueType v;
  index = node->FindValue(key, v, comparator_);
  if (index == -1) {
    return 2;  // 找不到这个key
  }
  if (comparator_(key, node->KeyAt(index)) != 0) {
    return 2;  // key不存在
  }
  if (!tree_page->IsDeleteSafe()) {
    return 3;
  }
  for (int i = index; i < node->GetSize() - 1; i++) {
    node->SetKeyAt(i, node->KeyAt(i + 1));
    node->SetValueAt(i, node->ValueAt(i + 1));
  }
  node->IncreaseSize(-1);

  lead_page_guard.Drop();
  return 0;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  // std::unique_lock<std::mutex> lq(mutex_);
  Context ctx;
  (void)ctx;
  LOG_INFO("Remove key : %s", std::to_string(key.ToString()).c_str());
  // int flag = HappyRemove(key, txn);
  // if (flag == 2 || flag == 1 || flag == 0) {  // key不存在  或者 空树
  //   return;
  // }

  // LOG_INFO("Remove key : %s", std::to_string(key.ToString()).c_str());
  std::map<page_id_t, int> index_mp;

  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = head_page->root_page_id_;

  WritePageGuard guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  auto tree_node = guard.AsMut<BPlusTreePage>();
  // int num = 1;
  while (!tree_node->IsLeafPage()) {
    auto internal_node = guard.AsMut<InternalPage>();
    page_id_t value;

    int index = internal_node->FindValue(key, value, comparator_);
    if (internal_node->GetSize() < 2) {
      throw Exception("异常:1011");
    }
    // num++;
    index_mp[internal_node->ValueAt(index)] = index;
    ctx.write_set_.push_back(std::move(guard));
    guard = bpm_->FetchPageWrite(value);
    tree_node = guard.AsMut<BPlusTreePage>();
  }
  auto leaf_node = guard.AsMut<LeafPage>();
  page_id_t this_page_id = guard.PageId();
  ctx.write_set_.push_back(std::move(guard));

  ValueType value;
  int index = leaf_node->FindValue(key, value, comparator_);

  if (index != -1 && (comparator_(leaf_node->KeyAt(index), key) == 0)) {
    value = leaf_node->ValueAt(index);
  } else {
    return;
  }
  DeleteLeafNodeKey(this_page_id, key, index_mp, ctx, txn);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteLeafNodeKey(page_id_t this_page_id, const KeyType &key, std::map<page_id_t, int> index_mp,
                                       Context &ctx, Transaction *txn) {
  WritePageGuard cur_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto node = cur_guard.AsMut<LeafPage>();

  if (ctx.write_set_.empty()) {
    if (node->GetSize() == 0) {
      LOG_INFO("叶节点为根的情况下:nodeSize=0");
      return;
    }
    if (node->GetSize() == 1) {
      LOG_INFO("叶节点为根的情况下:nodeSize=1,设置rootpageid =-1");
      WritePageGuard guard = std::move(ctx.header_page_.value());  //  先持有锁
      ctx.header_page_ = std::nullopt;
      auto head_node = guard.AsMut<BPlusTreeHeaderPage>();
      head_node->root_page_id_ = INVALID_PAGE_ID;
      ctx.root_page_id_ = INVALID_PAGE_ID;
      node->IncreaseSize(-1);
      return;
    }
    int index = -1;
    ValueType v;
    index = node->FindValue(key, v, comparator_);
    if (index == -1) {
      return;
    }
    if (index == -1 || (comparator_(node->KeyAt(index), key) != 0)) {
      return;
    }
    for (int i = index; i < node->GetSize() - 1; i++) {
      node->SetKeyAt(i, node->KeyAt(i + 1));
      node->SetValueAt(i, node->ValueAt(i + 1));
    }
    node->IncreaseSize(-1);
    return;
  }

  int index = -1;
  ValueType v;
  index = node->FindValue(key, v, comparator_);
  for (int i = index; i < node->GetSize() - 1; i++) {
    node->SetKeyAt(i, node->KeyAt(i + 1));
    node->SetValueAt(i, node->ValueAt(i + 1));
  }
  node->IncreaseSize(-1);

  if (node->GetSize() >= node->GetMinSize()) {  // 删掉之后还能维持树的形状
    return;
  }

  // 2.两侧节点可以安全删除一个 就借一个
  WritePageGuard &guard = ctx.write_set_.back();
  page_id_t parent_page_id = guard.PageId();
  auto parent_node = guard.AsMut<InternalPage>();
  int parent_index = (index_mp)[this_page_id];

  bool borrow_left = true;
  LeafPage *borrow_node = nullptr;

  if (parent_index == 0) {
    if (parent_index + 1 >= parent_node->GetSize()) {
      throw Exception("异常001");
    }
    WritePageGuard guard1 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index + 1));
    borrow_node = guard1.AsMut<LeafPage>();
    borrow_left = false;
  } else if (parent_index == parent_node->GetSize() - 1) {
    if (parent_index - 1 < 0) {
      throw Exception("异常002");
    }
    WritePageGuard guard1 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index - 1));
    borrow_node = guard1.AsMut<LeafPage>();
    borrow_left = true;
  } else {
    if (parent_index + 1 >= parent_node->GetSize()) {
      throw Exception("异常003");
    }
    if (parent_index - 1 < 0) {
      throw Exception("异常004");
    }
    WritePageGuard guard1 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index - 1));
    auto node_left = borrow_node = guard1.AsMut<LeafPage>();
    WritePageGuard guard2 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index + 1));
    auto node_right = borrow_node = guard2.AsMut<LeafPage>();
    if (node_left->GetSize() >= node_right->GetSize()) {
      borrow_node = node_left;
      borrow_left = true;
    } else {
      borrow_node = node_right;
      borrow_left = false;
    }
  }

  if (borrow_node->GetSize() - 1 < borrow_node->GetMinSize()) {
    // 合并
    int delete_up_index = -1;
    delete_up_index = parent_index + 1;
    if (borrow_left) {
      delete_up_index = parent_index;
      auto temp = node;
      node = borrow_node;
      borrow_node = temp;
    } else {
      delete_up_index = parent_index + 1;
    }

    int next_page_id = borrow_node->GetNextPageId();
    int num = 0;

    for (int i = node->GetSize(), j = 0; j < borrow_node->GetSize(); j++, i++) {
      node->SetKeyAt(i, borrow_node->KeyAt(j));
      node->SetValueAt(i, borrow_node->ValueAt(j));
      num++;
    }

    node->SetNextPageId(next_page_id);
    node->IncreaseSize(num);

    DeleteInternalNodeKey(parent_page_id, delete_up_index, index_mp, ctx, txn);

    return;
  }

  if (borrow_left) {
    if (borrow_node->GetSize() - 1 == -1 || borrow_node->GetSize() - 1 >= borrow_node->GetSize()) {
      throw Exception("异常11111");
    }
    KeyType borrow_key = borrow_node->KeyAt(borrow_node->GetSize() - 1);
    ValueType borrow_value = borrow_node->ValueAt(borrow_node->GetSize() - 1);
    borrow_node->IncreaseSize(-1);
    InsertLeafNode(node, borrow_key, borrow_value, ctx);
    parent_node->SetKeyAt(parent_index, node->KeyAt(0));
  } else {
    KeyType borrow_key = borrow_node->KeyAt(0);
    ValueType borrow_value = borrow_node->ValueAt(0);
    for (int i = 1; i < borrow_node->GetSize(); i++) {
      if (i >= borrow_node->GetSize()) {
        throw Exception("异常22222");
      }
      borrow_node->SetKeyAt(i - 1, borrow_node->KeyAt(i));
      borrow_node->SetValueAt(i - 1, borrow_node->ValueAt(i));
    }
    borrow_node->IncreaseSize(-1);
    node->SetKeyAt(node->GetSize(), borrow_key);
    node->SetValueAt(node->GetSize(), borrow_value);
    node->IncreaseSize(1);
    parent_node->SetKeyAt(parent_index + 1, borrow_node->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteInternalNodeKey(bustub::page_id_t this_page_id, int delete_index,
                                           std::map<page_id_t, int> index_mp, bustub::Context &ctx,
                                           bustub::Transaction *txn) {
  WritePageGuard cur_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto node = cur_guard.AsMut<InternalPage>();

  if (ctx.write_set_.empty()) {
    if (node->GetSize() == 1) {
      throw Exception("异常333");
    }
    if (node->GetSize() == 2) {
      LOG_INFO("减少树高");
      auto head_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();

      head_page->root_page_id_ = node->ValueAt(0);
      ctx.root_page_id_ = node->ValueAt(0);

      node->IncreaseSize(-1);
      cur_guard.Drop();
      return;
    }

    for (int i = delete_index; i < node->GetSize() - 1; i++) {
      if (i + 1 >= node->GetSize()) {
        throw Exception("异常444");
      }
      node->SetKeyAt(i, node->KeyAt(i + 1));
      node->SetValueAt(i, node->ValueAt(i + 1));
    }
    node->IncreaseSize(-1);

    return;
  }

  for (int i = delete_index; i < node->GetSize() - 1; i++) {
    if (i + 1 >= node->GetSize()) {
      throw Exception("异常4444");
    }
    node->SetKeyAt(i, node->KeyAt(i + 1));
    node->SetValueAt(i, node->ValueAt(i + 1));
  }
  node->IncreaseSize(-1);
  if (node->GetSize() >= node->GetMinSize()) {
    return;
  }

  // 2.两侧节点可以安全删除一个 就借一个
  WritePageGuard &guard = ctx.write_set_.back();
  page_id_t parent_page_id = guard.PageId();
  auto parent_node = guard.AsMut<InternalPage>();

  int parent_index = (index_mp)[this_page_id];
  //  int old_node_size = node->GetSize();
  bool borrow_left = true;

  InternalPage *borrow_node = nullptr;

  if (parent_index == 0) {
    if (parent_index + 1 >= parent_node->GetSize()) {
      throw Exception("异常555");
    }
    WritePageGuard guard1 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index + 1));
    borrow_node = guard1.AsMut<InternalPage>();
    borrow_left = false;
  } else if (parent_index == parent_node->GetSize() - 1) {
    if (parent_index - 1 < 0) {
      throw Exception("异常666");
    }

    WritePageGuard guard1 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index - 1));
    borrow_node = guard1.AsMut<InternalPage>();
    borrow_left = true;
  } else {
    if (parent_index + 1 >= parent_node->GetSize()) {
      throw Exception("异常777");
    }
    if (parent_index - 1 < 0) {
      throw Exception("异常888");
    }
    WritePageGuard guard1 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index - 1));
    auto node_left = borrow_node = guard1.AsMut<InternalPage>();
    WritePageGuard guard2 = bpm_->FetchPageWrite(parent_node->ValueAt(parent_index + 1));
    auto node_right = borrow_node = guard2.AsMut<InternalPage>();
    if (node_left->GetSize() >= node_right->GetSize()) {
      borrow_node = node_left;
      borrow_left = true;
    } else {
      borrow_node = node_right;
      borrow_left = false;
    }
  }

  if (borrow_node->GetSize() - 1 >= borrow_node->GetMinSize()) {
    // 借一个删除

    if (borrow_left) {
      KeyType up_key = borrow_node->KeyAt(borrow_node->GetSize() - 1);
      KeyType down_key = parent_node->KeyAt(parent_index);
      page_id_t down_value = borrow_node->ValueAt(borrow_node->GetSize() - 1);
      borrow_node->IncreaseSize(-1);

      node->SetKeyAt(0, down_key);
      for (int i = node->GetSize(); i >= 1; i--) {
        node->SetKeyAt(i, node->KeyAt(i - 1));
        node->SetValueAt(i, node->ValueAt(i - 1));
      }
      node->SetValueAt(0, down_value);
      node->IncreaseSize(1);

      parent_node->SetKeyAt(parent_index, up_key);

    } else {
      KeyType up_key = borrow_node->KeyAt(1);

      KeyType down_key = parent_node->KeyAt(parent_index + 1);
      page_id_t down_value = borrow_node->ValueAt(0);
      for (int i = 1; i < borrow_node->GetSize(); i++) {
        borrow_node->SetKeyAt(i - 1, borrow_node->KeyAt(i));
        borrow_node->SetValueAt(i - 1, borrow_node->ValueAt(i));
      }
      borrow_node->IncreaseSize(-1);
      node->SetKeyAt(node->GetSize(), down_key);
      node->SetValueAt(node->GetSize(), down_value);
      node->IncreaseSize(1);

      parent_node->SetKeyAt(parent_index + 1, up_key);
    }

    return;
  }

  // 内部节点的合并

  if (!borrow_left) {
    KeyType down_key = parent_node->KeyAt(parent_index + 1);
    int num = 0;
    for (int i = node->GetSize(), j = 0; j < borrow_node->GetSize(); i++, j++) {
      if (j == 0) {
        node->SetKeyAt(i, down_key);
        node->SetValueAt(i, borrow_node->ValueAt(j));
      } else {
        node->SetKeyAt(i, borrow_node->KeyAt(j));
        node->SetValueAt(i, borrow_node->ValueAt(j));
      }
      num++;
    }
    node->IncreaseSize(num);

    int delete_index_in_parent = parent_index + 1;
    DeleteInternalNodeKey(parent_page_id, delete_index_in_parent, index_mp, ctx, txn);

  } else {
    auto temp = node;
    node = borrow_node;
    borrow_node = temp;

    KeyType down_key = parent_node->KeyAt(parent_index);
    int num = 0;
    for (int i = node->GetSize(), j = 0; j < borrow_node->GetSize(); i++, j++) {
      if (j == 0) {
        node->SetKeyAt(i, down_key);
        node->SetValueAt(i, borrow_node->ValueAt(j));
      } else {
        node->SetKeyAt(i, borrow_node->KeyAt(j));
        node->SetValueAt(i, borrow_node->ValueAt(j));
      }
      num++;
    }
    node->IncreaseSize(num);

    int delete_index_in_parent = parent_index;
    DeleteInternalNodeKey(parent_page_id, delete_index_in_parent, index_mp, ctx, txn);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // throw Exception("暂时不实现迭代器，查看是否是死锁和内存泄露原因");

  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_node = head_guard.As<BPlusTreeHeaderPage>();

  if (head_node->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard guard = bpm_->FetchPageRead(head_node->root_page_id_);
  page_id_t next_page_id;
  auto tree_page = guard.As<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_node = guard.As<InternalPage>();
    next_page_id = internal_node->ValueAt(0);
    guard = bpm_->FetchPageRead(next_page_id);
    tree_page = guard.As<BPlusTreePage>();
  }
  next_page_id = guard.PageId();
  auto leaf_node = guard.As<LeafPage>();
  MappingType entry = MappingType(leaf_node->KeyAt(0), leaf_node->ValueAt(0));
  return INDEXITERATOR_TYPE(bpm_, next_page_id, 0, entry);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // throw Exception("暂时不实现迭代器，查看是否是死锁和内存泄露原因");

  ReadPageGuard head_gurad = bpm_->FetchPageRead(header_page_id_);
  auto head_node = head_gurad.As<BPlusTreeHeaderPage>();

  if (head_node->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard guard = bpm_->FetchPageRead(head_node->root_page_id_);
  page_id_t next_page_id;
  auto tree_page = guard.As<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_node = guard.As<InternalPage>();
    internal_node->FindValue(key, next_page_id, comparator_);
    guard = bpm_->FetchPageRead(next_page_id);
    tree_page = guard.As<BPlusTreePage>();
  }
  auto leaf_node = guard.As<LeafPage>();
  next_page_id = guard.PageId();
  ValueType value;
  int index = leaf_node->FindValue(key, value, comparator_);
  if (index == -1) {
    return End();
  }
  if ((comparator_(leaf_node->KeyAt(index), key) != 0)) {
    return End();
  }
  MappingType entry = MappingType(leaf_node->KeyAt(index), leaf_node->ValueAt(index));

  return INDEXITERATOR_TYPE(bpm_, next_page_id, index, entry);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // throw Exception("暂时不实现迭代器，查看是否是死锁和内存泄露原因");
  Context ctx;
  return INDEXITERATOR_TYPE(bpm_, -1, 0);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

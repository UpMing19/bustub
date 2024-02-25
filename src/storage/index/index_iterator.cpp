/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bufferPoolManager, page_id_t pid, int index) {
  bpm_ = bufferPoolManager;
  pid_ = pid;
  index_ = index;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return pid_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  ReadPageGuard guard = bpm_->FetchPageRead(pid_);
  auto node = guard.As<LeafPage>();
  KeyType k = node->KeyAt(index_);
  ValueType v = node->ValueAt(index_);
  entry_.first = k;
  entry_.second = v;
  return entry_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (pid_ == -1) {
    index_ = 0;
    return *this;
  }
  ReadPageGuard guard = bpm_->FetchPageRead(pid_);
  auto node = guard.As<LeafPage>();
  if (index_ + 1 < node->GetSize()) {
    index_++;
  } else if (node->GetNextPageId() != -1) {
    guard = bpm_->FetchPageRead(node->GetNextPageId());
    pid_ = guard.PageId();
    index_ = 0;
  } else {
    pid_ = -1;
    index_ = 0;
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &that) -> bool {
  return ((pid_ == that.pid_) && (index_ == that.index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &that) -> bool {
  return ((pid_ != that.pid_) || (index_ != that.index_));
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

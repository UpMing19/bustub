/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bufferPoolManager, page_id_t pid, int index, MappingType &entry)
    : bpm_(bufferPoolManager), pid_(pid), index_(index) {
  entry_ = entry;
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : bpm_(buffer_pool_manager), pid_(page_id), index_(index) {}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return pid_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return entry_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (pid_ == -1) {
    index_ = 0;
    return *this;
  }
  WritePageGuard guard = bpm_->FetchPageWrite(pid_);
  auto node = guard.AsMut<LeafPage>();
  if (index_ + 1 < node->GetSize()) {
    index_++;
    entry_.first = node->KeyAt(index_);
    entry_.second = node->ValueAt(index_);
  } else if (node->GetNextPageId() != -1) {
    guard = bpm_->FetchPageWrite(node->GetNextPageId());
    node = guard.AsMut<LeafPage>();
    pid_ = guard.PageId();
    index_ = 0;
    entry_.first = node->KeyAt(index_);
    entry_.second = node->ValueAt(index_);
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

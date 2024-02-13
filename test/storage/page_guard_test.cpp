//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // // Shutdown the disk manager and remove the temporary file we created.
  // disk_manager->ShutDown();
}

// 参考https://www.cnblogs.com/1v7w/p/17620338.html
// 参考https://zhuanlan.zhihu.com/p/615312257
TEST(PageGuardTest, ReadTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // test ~ReadPageGuard()
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  // test ReadPageGuard(ReadPageGuard &&that)
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
    auto reader_guard_2 = ReadPageGuard(std::move(reader_guard));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // test ReadPageGuard::operator=(ReadPageGuard &&that)
  {
    auto reader_guard_1 = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(3, page0->GetPinCount());
    reader_guard_1 = std::move(reader_guard_2);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  // test ReadPageGuard::Drop()
  {
    auto reader_guard_1 = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(3, page0->GetPinCount());
    auto reader_guard_3 = std::move(reader_guard_1);
    EXPECT_EQ(3, page0->GetPinCount());
    reader_guard_1.Drop();
    EXPECT_EQ(3, page0->GetPinCount());
    reader_guard_3.Drop();
    EXPECT_EQ(2, page0->GetPinCount());
    reader_guard_3.Drop();
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, WriteTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // test ~WritePageGuard()
  {
    auto writer_guard = bpm->FetchPageWrite(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // test WritePageGuard(ReadPageGuard &&that)
  {
    auto writer_guard = bpm->FetchPageWrite(page_id_temp);
    auto writer_guard_2 = WritePageGuard(std::move(writer_guard));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

// 参考https://zhuanlan.zhihu.com/p/629006919
TEST(PageGuardTest, HHTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp = 0;
  page_id_t page_id_temp_a;
  auto *page0 = bpm->NewPage(&page_id_temp);
  auto *page1 = bpm->NewPage(&page_id_temp_a);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  auto guarded_page_a = BasicPageGuard(bpm.get(), page1);

  // after drop, whether destructor decrements the pin_count_ ?
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard1.Drop();
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(1, page1->GetPinCount());
  // test the move assignment
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard2 = std::move(read_guard1);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  // test the move constructor
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2(std::move(read_guard1));
    auto read_guard3(std::move(read_guard2));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page_id_temp, page0->GetPageId());

  // repeat drop
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());

  disk_manager->ShutDown();
}

TEST(PageGuardTest, MoveCopyTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t core_page_id;
  auto core_page = bpm->NewPage(&core_page_id);
  {
    auto cure_guard = BasicPageGuard(bpm.get(), core_page);
    // core_page = nullptr;
    for (int i = 1; i <= 10; ++i) {
      auto dummy_guard1 = bpm->FetchPageBasic(core_page_id);
      EXPECT_EQ(2, core_page->GetPinCount());
      auto dummy_guard2(std::move(dummy_guard1));
      EXPECT_EQ(2, core_page->GetPinCount());
    }
  }
  auto dummy_page = bpm->NewPage(&core_page_id);
  EXPECT_EQ(1, dummy_page->GetPinCount());

  disk_manager->ShutDown();
}

TEST(PageGuardTest, MutliThreadTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t core_page_id;
  auto core_page = bpm->NewPage(&core_page_id);

  auto worker = [core_page_id](const std::shared_ptr<BufferPoolManager> &bpm) {
    const int n_level2 = 42;
    std::vector<std::thread> threads(n_level2);
    // mutli base guard
    for (int i = 0; i < n_level2; ++i) {
      threads.emplace_back([core_page_id, bpm] { auto dummy = bpm->FetchPageBasic(core_page_id); });
    }
    // mutli read guard
    for (int i = 0; i < n_level2; ++i) {
      threads.emplace_back([core_page_id, bpm] { auto dummy = bpm->FetchPageRead(core_page_id); });
    }

    for (int i = 0; i < n_level2; ++i) {
      threads.emplace_back([core_page_id, bpm] {
        auto dummy = bpm->FetchPageBasic(core_page_id);
        auto dummy2(std::move(dummy));
      });
    }
    for (int i = 0; i < n_level2; ++i) {
      threads.emplace_back([core_page_id, bpm] {
        auto dummy = bpm->FetchPageBasic(core_page_id);
        auto dummy2 = bpm->FetchPageBasic(core_page_id);
        dummy2 = std::move(dummy);
      });
    }

    auto dummy1 = bpm->FetchPageWrite(core_page_id);
    auto dummy2(std::move(dummy1));
    dummy1 = std::move(dummy2);

    for (auto &t : threads) {
      if (t.joinable()) {
        t.join();
      }
    }
  };

  const int n_level1 = 42;
  std::vector<std::thread> threads(n_level1);
  for (int i = 0; i < n_level1; ++i) {
    threads.emplace_back(worker, bpm);
  }

  for (auto &t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  EXPECT_EQ(1, core_page->GetPinCount());

  disk_manager->ShutDown();
}

TEST(PageGuardTest, MutliThreadRandPageTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;
  const int page_size = 20;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<int> uniform_dist(0, page_size - 1);
  // auto rand_page = static_cast<page_id_t>(uniform_dist(rng));

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t core_page_id;
  auto core_page = bpm->NewPage(&core_page_id);
  EXPECT_NE(nullptr, core_page);
  for (int i = 1; i < page_size; ++i) {
    page_id_t dummy_page_id;
    auto dummy_page = bpm->NewPage(&dummy_page_id);
    EXPECT_NE(nullptr, dummy_page);
    bpm->UnpinPage(dummy_page_id, false);
  }

  auto worker = [uniform_dist, rng](const std::shared_ptr<BufferPoolManager> &bpm) mutable {
    const int n_level2 = 42;
    std::vector<std::thread> threads(n_level2);
    for (int i = 0; i < n_level2; ++i) {
      threads.emplace_back([uniform_dist, rng, bpm]() mutable {
        auto rand_page = static_cast<page_id_t>(uniform_dist(rng));
        assert(0 <= rand_page && rand_page < page_size);
        auto dummy = bpm->FetchPageRead(rand_page);
      });
    }
    for (int i = 0; i < n_level2; ++i) {
      threads.emplace_back([uniform_dist, rng, bpm]() mutable {
        auto rand_page = static_cast<page_id_t>(uniform_dist(rng));
        assert(0 <= rand_page && rand_page < page_size);
        auto dummy = bpm->FetchPageBasic(rand_page);
      });
    }
    for (int i = 0; i < n_level2; ++i) {
      threads.emplace_back([uniform_dist, rng, bpm]() mutable {
        auto rand_page = static_cast<page_id_t>(uniform_dist(rng));
        assert(0 <= rand_page && rand_page < page_size);
        auto dummy = bpm->FetchPageBasic(rand_page);
      });
    }
    for (auto &t : threads) {
      if (t.joinable()) {
        t.join();
      }
    }
  };

  const int n_level1 = 42;
  std::vector<std::thread> threads(n_level1);
  for (int i = 0; i < n_level1; ++i) {
    threads.emplace_back(worker, bpm);
  }

  for (auto &t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  EXPECT_EQ(1, core_page->GetPinCount());

  disk_manager->ShutDown();
}

// from https://github.com/ZhaoHaoRu/CMU15445-learning-record/blob/main/testcase/storage/page_guard_test.cpp
TEST(PageGuardTest, BPMTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto basic_guard = bpm->NewPageGuarded(&page_id_temp);

  auto page0 = bpm->FetchPage(0);
  EXPECT_NE(nullptr, page0);
  EXPECT_EQ(2, page0->GetPinCount());
  EXPECT_EQ(basic_guard.PageId(), page0->GetPageId());

  basic_guard.Drop();
  EXPECT_EQ(1, page0->GetPinCount());

  auto basic_guard2 = bpm->FetchPageBasic(0);
  basic_guard2.Drop();
  EXPECT_EQ(1, page0->GetPinCount());

  bpm->UnpinPage(page0->GetPageId(), page0->IsDirty());

  for (int i = 1; i <= 10; ++i) {
    bpm->NewPage(&page_id_temp);
  }

  for (size_t i = 1; i <= buffer_pool_size; ++i) {
    auto ok = bpm->UnpinPage(i, false);
    ASSERT_EQ(true, ok);
  }
  page0 = bpm->FetchPage(0);
  return;
  ASSERT_NE(nullptr, page0);

  { auto read_guard = bpm->FetchPageRead(0); }

  auto write_guard = bpm->FetchPageWrite(0);
  // auto write_guard2 = bpm->FetchPageWrite(0);
  auto basic_guard3 = bpm->FetchPageBasic(0);
  ASSERT_EQ(page0->GetPinCount(), 3);
}

}  // namespace bustub

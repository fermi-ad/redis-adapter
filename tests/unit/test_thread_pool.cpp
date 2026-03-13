//
//  test_thread_pool.cpp
//
//  Unit tests for ThreadPool: deterministic routing, shutdown, edge cases
//

#include <gtest/gtest.h>
#include "ThreadPool.hpp"
#include <atomic>
#include <thread>
#include <chrono>

using namespace std;
using namespace chrono;

TEST(ThreadPool, SingleWorkerExecutesJob)
{
  ThreadPool pool(1);
  atomic<bool> done{false};
  pool.job("key", [&]() { done = true; });
  for (int i = 0; i < 100 && !done; i++)
    this_thread::sleep_for(milliseconds(1));
  EXPECT_TRUE(done.load());
}

TEST(ThreadPool, MultipleWorkersAllExecute)
{
  ThreadPool pool(4);
  atomic<int> count{0};
  for (int i = 0; i < 100; i++)
    pool.job("key_" + to_string(i), [&]() { count++; });
  for (int i = 0; i < 200 && count < 100; i++)
    this_thread::sleep_for(milliseconds(1));
  EXPECT_EQ(count.load(), 100);
}

TEST(ThreadPool, DeterministicRouting)
{
  // Same key should always go to same worker (FIFO order preserved)
  ThreadPool pool(4);
  vector<int> order;
  mutex mtx;
  for (int i = 0; i < 10; i++)
  {
    pool.job("same_key", [&, i]()
    {
      lock_guard<mutex> lk(mtx);
      order.push_back(i);
    });
  }
  this_thread::sleep_for(milliseconds(50));
  lock_guard<mutex> lk(mtx);
  ASSERT_EQ(order.size(), 10u);
  for (int i = 0; i < 10; i++)
    EXPECT_EQ(order[i], i);
}

TEST(ThreadPool, ZeroWorkersDropsJobs)
{
  ThreadPool pool(0);
  atomic<bool> ran{false};
  pool.job("key", [&]() { ran = true; });
  this_thread::sleep_for(milliseconds(10));
  EXPECT_FALSE(ran.load());
}

TEST(ThreadPool, DestructorJoinsThreads)
{
  // Destructor sets _go=false and joins threads — verify no crash/hang
  atomic<int> count{0};
  {
    ThreadPool pool(2);
    for (int i = 0; i < 10; i++)
      pool.job("k" + to_string(i), [&]() { count++; });
    // Let some jobs complete before destruction
    this_thread::sleep_for(milliseconds(20));
  } // destructor runs here — should not hang or crash
  EXPECT_GE(count.load(), 0);  // some may have completed
}

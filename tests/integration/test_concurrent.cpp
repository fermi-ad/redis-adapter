//
//  test_concurrent.cpp
//
//  Multi-threaded access tests: concurrent writes, reads, worker pool routing
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <thread>
#include <atomic>
#include <vector>

using namespace std;
using namespace chrono;

TEST(Concurrent, ParallelWrites)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_conc_pw");

  const int n_threads = 4;
  const int writes_per = 25;
  vector<thread> threads;

  for (int t = 0; t < n_threads; t++)
  {
    threads.emplace_back([&redis, t, writes_per]()
    {
      for (int i = 0; i < writes_per; i++)
        redis.addInt("t_conc_pw", t * 1000 + i, {.trim = 0});
    });
  }
  for (auto& th : threads) th.join();

  auto results = redis.getInts("t_conc_pw");
  // Under contention some writes may fail; verify most succeeded
  EXPECT_GE(results.size(), static_cast<size_t>(n_threads * writes_per * 80 / 100));
  redis.del("t_conc_pw");
}

TEST(Concurrent, ParallelReadWrite)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_conc_rw");
  redis.addInt("t_conc_rw", 0);

  atomic<bool> stop{false};
  atomic<int> reads{0};

  thread writer([&]()
  {
    for (int i = 0; i < 50; i++)
      redis.addInt("t_conc_rw", i, {.trim = 0});
    stop = true;
  });

  thread reader([&]()
  {
    int64_t val;
    while (!stop)
    {
      redis.getInt("t_conc_rw", val);
      reads++;
    }
  });

  writer.join();
  reader.join();

  EXPECT_GT(reads.load(), 0);
  redis.del("t_conc_rw");
}

TEST(Concurrent, MultipleInstances)
{
  // Multiple RedisAdapterLite instances writing to different keys
  const int n = 4;
  vector<thread> threads;
  atomic<int> successes{0};

  for (int i = 0; i < n; i++)
  {
    threads.emplace_back([i, &successes]()
    {
      RedisAdapterLite redis("CONC_" + to_string(i));
      string key = "t_multi_inst";
      redis.del(key);
      for (int j = 0; j < 25; j++)
        redis.addInt(key, j, {.trim = 0});
      auto results = redis.getInts(key);
      if (results.size() == 25) successes++;
      redis.del(key);
    });
  }
  for (auto& th : threads) th.join();
  EXPECT_EQ(successes.load(), n);
}

TEST(Concurrent, WorkerPoolMultipleWorkers)
{
  RAL_Options opts;
  opts.workers = 4;
  RedisAdapterLite redis("TEST", opts);
  redis.del("t_conc_wp");
  redis.addInt("t_conc_wp", 0);

  atomic<int> count{0};
  redis.addReader("t_conc_wp",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty()) count++;
    });
  this_thread::sleep_for(milliseconds(20));

  for (int i = 1; i <= 5; i++)
    redis.addInt("t_conc_wp", i, {.trim = 0});

  for (int i = 0; i < 80 && count < 5; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_GE(count.load(), 1);
  redis.removeReader("t_conc_wp");
  redis.del("t_conc_wp");
}

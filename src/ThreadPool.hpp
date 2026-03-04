//
//  ThreadPool.hpp
//
//  Worker thread pool that dispatches jobs deterministically by name hash.
//  Uses atomic _go flag for thread safety.
//

#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <string>
#include <vector>
#include <atomic>

class ThreadPool
{
public:
  ThreadPool(unsigned short num) : _workers(num)
  {
    for (auto& w : _workers)
      { w._thd = std::thread(&Worker::work, &w); }
  }

  ~ThreadPool()
  {
    for (auto& w : _workers)
    {
      w._go = false;
      w._cv.notify_all();
    }
    for (auto& w : _workers)
      { if (w._thd.joinable()) w._thd.join(); }
  }

  void job(const std::string& name, std::function<void(void)> func)
  {
    static std::hash<std::string> hasher;

    size_t num = _workers.size();
    if (num == 0) return;

    size_t idx = (num == 1) ? 0 : hasher(name) % num;
    Worker& w = _workers[idx];

    {
      std::lock_guard<std::mutex> lk(w._mtx);
      w._jobs.emplace(std::move(func));
    }
    w._cv.notify_one();
  }

private:
  struct Worker
  {
    std::atomic<bool> _go{true};
    std::mutex _mtx;
    std::thread _thd;
    std::condition_variable _cv;
    std::queue<std::function<void(void)>> _jobs;

    void work()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      while (_go)
      {
        while (_go && _jobs.empty()) { _cv.wait(lk); }

        if (!_jobs.empty())
        {
          auto job = std::move(_jobs.front());
          _jobs.pop();

          lk.unlock();
          job();
          lk.lock();
        }
      }
    }
  };

  std::vector<Worker> _workers;
};

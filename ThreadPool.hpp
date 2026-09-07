#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <syslog.h>
#include <exception>
#include <memory>
#include <string>
#include <vector>

class ThreadPool
{
public:
  ThreadPool(unsigned short num) : _workers(num)
  {
    try {
      for (auto& worker : _workers) {
        worker = std::make_shared<Worker>();
        const auto index = --num;
        worker->_thd = std::thread([keep = worker, index]() { keep->work(index); });
      }
    } catch (...) {
      stop();
      throw;
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ~ThreadPool() { stop(); }

  void job(const std::string& name, std::function<void(void)> func)
  {
    static std::hash<std::string> hasher;

    int idx = 0;
    size_t num = _workers.size();

    switch (num)
    {
      //  no workers
      case 0: return;
      //  one worker - no need to hash
      case 1: break;
      //  assign job to thread deterministically by name hash
      default: idx = hasher(name) % num; break;
    }
    const auto keep = _workers[idx];
    Worker& w = *keep;

    std::unique_lock<std::mutex> lk(w._mtx);
    if (!w._go) return;
    w._jobs.emplace(std::move(func));
    lk.unlock();

    w._cv.notify_all();
  }

private:
  struct Worker
  {
    bool _go = true;
    std::mutex _mtx;
    std::thread _thd;
    std::condition_variable _cv;
    std::queue<std::function<void(void)>> _jobs;

    void work(unsigned short num)
    {
      std::unique_lock<std::mutex> lk(_mtx);
      while (_go)
      {
        //  note cv unlocks mutex while waiting, relocks when done
        while (_go && _jobs.empty()) { _cv.wait(lk); }

        if (!_go) break;
        if (_jobs.size())
        {
          auto job = std::move(_jobs.front());
          _jobs.pop();

          // syslog(LOG_INFO, "worker %u has job", num);

          lk.unlock();
          try {
            job();
          } catch (const std::exception& ex) {
            syslog(LOG_ERR, "stream callback failed: %s", ex.what());
          } catch (...) {
            syslog(LOG_ERR, "stream callback failed with unknown exception");
          }
          lk.lock();
        }
      }
    }
  };

  void stop() noexcept
  {
    for (const auto& worker : _workers) {
      if (!worker) continue;
      {
        std::lock_guard<std::mutex> guard(worker->_mtx);
        worker->_go = false;
      }
      worker->_cv.notify_all();
    }
    for (const auto& worker : _workers) {
      if (!worker || !worker->_thd.joinable()) continue;
      if (worker->_thd.get_id() == std::this_thread::get_id()) {
        // The thread's lambda retains this Worker's storage until work() exits.
        worker->_thd.detach();
      } else {
        worker->_thd.join();
      }
    }
  }

  std::vector<std::shared_ptr<Worker>> _workers;
};

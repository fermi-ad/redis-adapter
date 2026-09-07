#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <syslog.h>
#include <exception>

class ThreadPool
{
public:
  ThreadPool(unsigned short num) : _workers(num)
  {
    for (auto& w : _workers)
      { w._thd = std::thread(&Worker::work, &w, --num); }
  }

  ~ThreadPool()
  {
    for (auto& w : _workers)
    {
      {
        std::lock_guard<std::mutex> guard(w._mtx);
        w._go = false;
      }
      w._cv.notify_all();
    }
    for (auto& w : _workers)
      { if (w._thd.joinable()) w._thd.join(); }
  }

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
    Worker& w = _workers[idx];

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

  std::vector<Worker> _workers;
};

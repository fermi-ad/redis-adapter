#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>

class ThreadPool
{
public:
  ThreadPool(int num)
  {
    _workers.reserve(num);
    while (num--)
    {
      Worker& w = _workers.emplace_back(Worker());
      w.thd = std::thread(std::bind(&Worker::work, &w));
    }
  }
  ~ThreadPool()
  {
    for (auto& w : _workers)
    {
      do
      {
        std::unique_lock<std::mutex> lk(w.lock);
        w.go = false;
        w.cv.notify_all();
      }
      while (false);
    }
    for (auto& w : _workers)
      { if (w.thd.joinable()) w.thd.join(); }
  }
  void job(const std::string& name, std::function<void(void)> func)
  {
    Worker& w = _workers[_hasher(name) % _workers.size()];
    do
    {
      std::unique_lock<std::mutex> lk(w.lock);
      w.jobs.emplace(std::move(func));
    }
    while (false);
    w.cv.notify_one();
  }

private:
  struct Worker
  {
    Worker() : go(true) {}
    Worker(const Worker&) = delete;
    Worker(Worker&& w) {}

    bool go;
    std::mutex lock;
    std::condition_variable cv;
    std::queue<std::function<void(void)>> jobs;
    std::thread thd;

    void work()
    {
      for (std::function<void(void)> job; go; /**/)
      {
        do
        {
          std::unique_lock<std::mutex> lk(lock);
          while (go && jobs.empty())
            { cv.wait(lk); }

          if (jobs.empty()) return;

          job = std::move(jobs.front());
          jobs.pop();
        }
        while (false);
        job();
      }
    }
  };
  std::vector<Worker> _workers;
  std::hash<std::string> _hasher;
};

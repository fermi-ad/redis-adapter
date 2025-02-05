#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>

class ThreadPool
{
public:
  ThreadPool(unsigned short num) : _workers(num)
  {
    for (auto& w : _workers)
      { w._thd = std::thread(std::bind(&Worker::work, &w, --num)); }
  }

  ~ThreadPool()
  {
    for (auto& w : _workers)
    {
      //  no need to lock here
      w._go = false;
      w._cv.notify_all();
    }
    for (auto& w : _workers)
      { if (w._thd.joinable()) w._thd.join(); }
  }

  void job(const std::string& name, std::function<void(void)> func)
  {
    //  assign job to thread deterministically by name hash
    Worker& w = _workers[_hasher(name) % _workers.size()];
    std::unique_lock<std::mutex> lk(w._mtx);
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
          job();  //  do the job while unlocked
          lk.lock();
        }
      }
    }
  };

  std::vector<Worker> _workers;
  std::hash<std::string> _hasher;
};

#include "ThreadPool.hpp"
#include <gtest/gtest.h>
#include <atomic>
#include <future>
#include <memory>
#include <stdexcept>

using namespace std::chrono_literals;

namespace {
using Parameters = std::pair<unsigned, bool>;
class ThreadPoolCapture : public testing::TestWithParam<Parameters> {};

struct OnDestroy {
  std::function<void()> action;
  ~OnDestroy() { action(); }
};

TEST_P(ThreadPoolCapture, CleanupMayEnqueue) {
  ThreadPool pool(GetParam().first);
  auto completed = std::make_shared<std::promise<void>>();
  auto done = completed->get_future();
  std::promise<void> release;
  auto ready = release.get_future().share();
  auto captured = std::make_shared<OnDestroy>();
  captured->action = [&pool, completed] {
    // The same key selects the currently executing worker at every pool size.
    pool.job("capture", [completed] { completed->set_value(); });
  };
  pool.job("capture", [captured, ready, throwing = GetParam().second] {
    ready.wait();
    if (throwing) throw std::runtime_error("intentional callback exception");
  });
  captured.reset();
  release.set_value();
  ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
}

struct LastPoolOwner {
  std::shared_ptr<ThreadPool> owner;
  std::shared_ptr<std::promise<void>> completed;
  std::shared_ptr<std::atomic<bool>> queuedDestroyed;
  std::shared_ptr<std::atomic<bool>> releasedBeforeReturn;
  ~LastPoolOwner() {
    owner.reset();
    if (queuedDestroyed) *releasedBeforeReturn = queuedDestroyed->load();
    completed->set_value();
  }
};

void lastOwner(unsigned workers, bool throwing, bool queued) {
  auto pool = std::make_shared<ThreadPool>(workers);
  auto completed = std::make_shared<std::promise<void>>();
  auto done = completed->get_future();
  std::promise<void> release;
  auto ready = release.get_future().share();
  auto captured = std::make_shared<LastPoolOwner>();
  captured->owner = pool;
  captured->completed = completed;
  auto destroyed = std::make_shared<std::atomic<bool>>(false);
  auto releasedBeforeReturn = std::make_shared<std::atomic<bool>>(false);
  if (queued) {
    captured->queuedDestroyed = destroyed;
    captured->releasedBeforeReturn = releasedBeforeReturn;
  }
  pool->job("capture", [captured, ready, throwing] {
    ready.wait();
    if (throwing) throw std::runtime_error("intentional callback exception");
  });
  if (queued) {
    auto waiting = std::make_shared<OnDestroy>();
    waiting->action = [destroyed] { *destroyed = true; };
    pool->job("capture", [waiting] {});
    waiting.reset();
  }
  captured.reset();
  pool.reset();
  release.set_value();
  ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
  if (queued) EXPECT_TRUE(releasedBeforeReturn->load());
}

TEST_P(ThreadPoolCapture, LastOwnerReleasedByCaptureCleanup) {
  lastOwner(GetParam().first, GetParam().second, false);
}

TEST_P(ThreadPoolCapture, CancelledCapturesReleasedBeforePoolDestructionReturns) {
  lastOwner(GetParam().first, GetParam().second, true);
}

INSTANTIATE_TEST_SUITE_P(SizesAndExceptions, ThreadPoolCapture,
                        testing::Values(Parameters{1, false}, Parameters{1, true},
                                        Parameters{4, false}, Parameters{4, true}));
}  // namespace

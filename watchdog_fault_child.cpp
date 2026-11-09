// Child for watchdog_failure_test.py: keeps an automatic watchdog alive through the proxy
// until it is killed.
//   REDIS_ADAPTER_TEST_PORT        port to connect to (the fault-injecting proxy)
//   REDIS_ADAPTER_WATCHDOG_BASE    base key
#include "RedisAdapter.hpp"
#include <cstdlib>
#include <unistd.h>

int main()
{
  const char* port = std::getenv("REDIS_ADAPTER_TEST_PORT");
  const char* base = std::getenv("REDIS_ADAPTER_WATCHDOG_BASE");
  if (!port || !base) return 2;

  RA_Options options;
  options.cxn.port = std::atoi(port);
  options.dogname = "dog";
  RedisAdapter redis(base, options);
  for (;;) pause();
}

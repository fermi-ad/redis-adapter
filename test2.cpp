
#include "RedisAdapter.hpp"

int main(int argc, char* argv[])
{
  printf("TEST2 - Simple Redis tests\n");

  RedisAdapterSingle redis("TEST2");

  printf("setDevice: device1 device2\n");
  redis.setDevice("device1");
  redis.setDevice("device2");

  auto devs = redis.getDevices();

  printf("getDevices: ");
  for (auto d : devs) printf("%s ", d.c_str());
  printf("\n");

  return 0;
}

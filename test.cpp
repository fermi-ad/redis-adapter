/**

 * test.c

 *

 * This module contains the entry point for the mm32xdaq_lxrt test application.

 */

#include "RedisAdapter.hpp"

#include <iostream>

using namespace std;

/**
 * Main
 *
 * A test application for interfacing with the redis adapter.
 * Including static calls, and subscriptions
 */

int main( int argc, char* argv[] ){

  printf( "\n*** RedisAdapter Test Application ***\n\n" );
  printf( "Acquiring Messages for 10 seconds.\n\n" );

  //Create a new adapter with base key TEST
  IRedisAdapter* redisAdapter = new RedisAdapterCluster("TEST");

  //Redis Adapter will connect to a redis config database at 127.0.0.1:6379
  //and a redsi cluster database at 127.0.0.1:30001

  static double time_old = 0.0;
  static uint32_t totalEvents = 0;
  //Subscribe to the 0x02 event, pass a callback as a lambda or a free function
  redisAdapter->subscribe("TCLK:02", [&](string key, string msg){
  									double time_new;
  									double delta_time;
  									struct timeval tv;
  									struct timezone tz;
  									gettimeofday( &tv, &tz );
  									time_new = (float)tv.tv_sec + ((float)tv.tv_usec / 1000000.0);
  									delta_time = time_new - time_old;
  									time_old = time_new;
									string output = "Event 0x02 received: " + key + " Time: " + msg;
 									printf("%s",output.c_str());
									printf("\nTime Since Last 0x02 : %f sec" ,delta_time);
  });

  //Pattern Subscribe to messages, similar call as above

  redisAdapter->psubscribe("TCLK:*", [&](string pattern, string key, string msg){ ++totalEvents; if (totalEvents % 5) printf("Total Events Received: %d", totalEvents);});

  redisAdapter->startListener();

  for(int i = 0; i < 0; i++ ){
      sleep( 10 );
      printf( "I've been asleep for %d seconds.\n", ((i+1)*10) );
    }


  // Test Stream Write

  const int numThreads    = stoi(argv[1]);
  const int numXadds      = stoi(argv[2]);
  const int numWaveforms  = stoi(argv[3]);
  const int size          = stoi(argv[4]);
  std::vector<std::thread> threads;
  std::string ipBase = "tcp://192.168.0.";

  std::cout << "Number of Threads   : " << to_string(numThreads)<< std::endl;
  std::cout << "Number of Xadds (Pipeline)    : " << to_string(numXadds)<< std::endl;
  std::cout << "Number of Waveforms : " << to_string(numWaveforms)<< std::endl;
  std::cout << "Size of Waveform    : " << to_string(size) <<" Floats" << std::endl;

  // Start up 30 threads, each with a RedisAdapter at an incrementing IP address
  auto allstart = std::chrono::high_resolution_clock::now();
  for (int thread = 1; thread <= numThreads; ++thread) {
    std::string ip = ipBase + std::to_string(thread);

    threads.emplace_back([ip, size, thread, numXadds, numWaveforms]() {
        RedisAdapterCluster adapter(ip);

        // Execute pipeline
        std::string key = "Key:" + ip;
        unsigned int seed = static_cast<unsigned int>(std::chrono::system_clock::now().time_since_epoch().count());
        seed += std::hash<std::thread::id>()(std::this_thread::get_id());
        srand(seed);

        // generate a random number between 1 and 1,000,000
        // int randomNumber = rand() % 1000000 + 1;

        string streamKey = key + ":pipe:" ;//+ to_string(randomNumber);
        auto pipe = adapter._redis.pipeline(streamKey);
        vector<float> vec2(size,9.99);
        for (int j = 0; j < numXadds; j++) {
          std::vector<std::pair<std::string, std::string>> attrs2 = {
            {"DATA", to_string(j)}
          };
          for( int z = 0; z < numWaveforms; ++z){
            attrs2.emplace_back( "Waveform"+to_string(z), string_view(reinterpret_cast<char*>(vec2.data()), vec2.size() * sizeof(float)));
          }
          pipe.xadd(streamKey, to_string(j)+"-1",  attrs2.begin(), attrs2.end());
        }
        auto results = pipe.exec();

    });
  }


  // Wait for all threads to finish
  for (auto& thread : threads) {
    thread.join();
  }
  auto allend = std::chrono::high_resolution_clock::now();
  auto allduration = std::chrono::duration_cast<std::chrono::milliseconds>(allend - allstart);
  std::cout << "All Stream Tests done in " << allduration.count() << " ms" << std::endl;

  return 0;
}



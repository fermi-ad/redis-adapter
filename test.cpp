/**

 * test.c

 *

 * This module contains the entry point for the mm32xdaq_lxrt test application.

 */

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/io.h>
#include <sys/time.h>
#include <unistd.h>
#include "IRedisAdapter.hpp"
#include "RedisAdapter.hpp"

/**
 * Main
 *
 * A test application for interfacing with the redis adapter.
 * Including static calls, and subscriptions
 */

int main( int argc, char* argv[] ){

  printf( "\n*** RedisAdapter Test Application ***\n\n" );
  printf( "Acquiring Messages for 60 seconds.\n\n" );

  //Create a new adapter with base key TEST
  IRedisAdapter* redisAdapter = new RedisAdapter("TEST");

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
  
  redisAdapter->psubscribe("TCLK:*", [&](string pattern, string key, string msg){ ++totalEvents; if (totalEvents % 27) printf("Total Events Received: %d", totalEvents);});

  for(int i = 0; i < 6; i++ ){
      sleep( 10 );
      printf( "I've been asleep for %d seconds.\n", ((i+1)*10) );
    }
  return 0;
}


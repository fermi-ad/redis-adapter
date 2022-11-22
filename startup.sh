#!/bin/bash

/usr/src/redisAdapter/redis-stable/utils/create-cluster/create-cluster start
echo yes | /usr/src/redisAdapter/redis-stable/utils/create-cluster/create-cluster create 

while true ; do
   echo " " ; sleep 300
   done
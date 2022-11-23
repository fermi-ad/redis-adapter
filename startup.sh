#!/bin/bash

./create-cluster start
if [ "$JOIN" == "t" ]; then
   echo yes | ./create-cluster create 
fi

while true ; do
   echo " " ; sleep 300
done
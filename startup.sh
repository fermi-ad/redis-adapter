#!/bin/bash

./create-cluster start
if [ "$JOIN" == "yes" ]; then
   ./create-cluster create -f
fi

while true ; do
   echo " " ; sleep 300
done
#!/bin/bash

for i in {1..100}
do
   # run a few in background
   for j in {1..9}
   do
      ./run-grpcurl.sh&
   done
   # wait until completed
   ./run-grpcurl.sh
   sleep 0.5
done

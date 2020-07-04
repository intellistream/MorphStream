#!/bin/bash

RANKINGS=/home/kaichi/Downloads/big-data-benchmark/rankings_1node.csv
VISITS=/home/kaichi/Downloads/big-data-benchmark/uservisits_1node.csv
APP=/home/kaichi/workspace/SampleEnclaveTest/app
#APP=/home/kaichi/workspace/hash-join-test/Debug/hash-join-test

for i in {1..18}
do
  SIZE="${i}0MB"
  head -c $SIZE $VISITS > /tmp/tmp.csv
  ROWS=$(wc -l /tmp/tmp.csv | awk '{print $1}')
  echo "Running on $SIZE MB file with $ROWS rows"
  $APP $RANKINGS 40000 1 $VISITS $ROWS 2
  echo
done

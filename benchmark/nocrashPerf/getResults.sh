#!/bin/bash
dir=${1?"No target given!"}

for cm in CrashStop FullSS ViewSS EpochSS
do
  for x in {1..25}/{0..2}.0
  do
    cat ${dir}/${cm}/$x | grep RPS | tail -n +135 | head -n 100| cut -f2 -d' '
  done | awk '{sum+=$1;count++}; END{printf("%9s%10.2f\n", "'$cm'", sum/count);}'
done

#!/bin/bash

mkdir -p links

for sat in net cpu
do
  for cm in FullSS ViewSS EpochSS
  do
    for fl in fc lc
    do
      for type in l f c
      do
        for prefix in "" D
        do
          ln -s "../1${prefix}_${sat}.out/$cm/$fl/${type}_avg" \
                links/"${prefix:-R}_${sat}_${cm}_${fl}_$type.pdf"
        done;
      done
    done
  done
done

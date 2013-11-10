#!/bin/bash

die(){
  echo -e '\n\n:(\n'
  exit 1
}

base=$(readlink -e $0)
base=${base%/*}

for d in 1_{net,cpu}.out {2,3}_all.out
do
  pushd $d >/dev/null || die
  
  for cm in FullSS ViewSS EpochSS
  do
    pushd $cm >/dev/null || die
    for fl in {fc,lc}
    do
      pushd $fl >/dev/null || die
        
        echo -ne "$d,$cm,$fl,"
        $base/parseRecStats.py recStats
      
      popd >/dev/null
    done
    popd >/dev/null
  done
  popd >/dev/null
done;
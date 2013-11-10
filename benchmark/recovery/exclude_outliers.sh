#!/bin/bash
count=$(wc -l recStats | cut -f1 -d' ')
let count--
toRem=$((count-100))
if (( toRem<0 ))
then
  echo "Missing tests!"
  exit 1
fi
top=$(( (toRem/2) ))

filter(){
  awk 'BEGIN {start=-'$top'}; {if(start<0) printf("%s ", $1); if(start>=100) printf("%s ", $1); start++;}'
}

move="$(cat recStats | FS=',' sort -t, -nk 2  | tr ',' ' ' | tail -n +2 | filter)"
target="$(readlink -e $(pwd))_excluded"
mkdir -p $target
mv $move $target
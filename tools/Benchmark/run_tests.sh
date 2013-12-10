#/bin/bash

if(($# == 0))
then
  echo "usage: $0 <test files>"
  exit 1;
fi

trap "killall -9 java; exit 1" SIGINT

for file in $*
do
  ./generic_gzip.sh ${file} | tee ${file}_result;
done

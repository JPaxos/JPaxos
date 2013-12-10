#!/bin/bash

if(($# != 2));
then
echo "usage: $0 <output dir> <request size>";
exit 1;
fi

CLIENTS=$(seq 1 1 100);
REQUESTS=100000;
REQUEST_SIZE=$2;
OUTPUT_DIR=$1;

for CLIENT_COUNT in $CLIENTS
do
{
  ./make_echo_client_test $REQUEST_SIZE $REQUESTS CLIENT_COUNT
} > $OUTPUT_DIR/${CLIENT_COUNT}_clients
done

#!/bin/sh
cd $( dirname $( readlink -f "$0" ) )
./jpaxosMClient "$@" | ./processClient.sh

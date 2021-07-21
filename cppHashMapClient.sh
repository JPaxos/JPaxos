#!/bin/sh
cd $( dirname $( readlink -f "$0" ) )
exec ./hashMapClient "$@" | ./processClient.sh

#!/bin/bash

recolor(){
	echo "s/\($1\)/[$2m\1[00m/g;"
}

awk '
/Connection from/ {next;}
/Received client request/ {next;}
/New client connection/ {next;}
/Enqueueing reply/ {next;}
/Passing request to be executed to service/ {next;}
/execution took/ {next;}
/Scheduling sending reply/ {next;}
/Executed request/ {next;}
/Client proxy not found, discarding reply/ {next;}
//
' "$@" | sort -n -s -k1,1 | sed '
s/RPS: \([0-9]*\.[0-9]*\)/RPS: [01;33m\1[00m/g;
'"
$(recolor 'PrepareOK' 35)
$(recolor 'Prepare[^Od]' 36)
$(recolor 'Suspecting . on view .' 31)
$(recolor 'Preparing view: .' 31)
$(recolor 'View prepared .' 30)
$(recolor 'FD has been informed about view .' 31)
$(recolor 'CatchUpSnapshot' '01;04;32')
$(recolor 'CatchUpResponse' '01;32')
$(recolor 'CatchUpQuery' '32')
"| less -RS

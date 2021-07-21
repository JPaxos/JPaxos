#!/bin/bash
ID=${1:-0}
IN=${2:-jpdb.sqlite3}
SMOOTH=${3:-0.2}

TOOLS=$(readlink -m $0)
TOOLS=${TOOLS%/*}

count=$(sqlite3 $IN 'select count(*) from dps where id='$ID';')

maxtime=$(sqlite3 $IN 'select max(time) from rps where id='$ID';')

# Y-range is 5/4 of 95th percentile
dpsY=$(bc <<< "5*$(sqlite3 $IN 'select dps from dps where id='$ID' order by dps desc limit '$((count/20))';'| tail -n1)/4")
rpsY=$(bc <<< "5*$(sqlite3 $IN 'select rps from rps where id='$ID' order by rps desc limit '$((count/20))';'| tail -n1)/4/1000")

function getRes(){
    sqlite3 -separator ' ' $IN "select time, printf(\"%.10f\", round($1/1000/1000/1000*8,9)) from netIptbl where id=$ID;"
    echo e
}

(
echo \
"
set terminal x11 title '${TITLE}$IN - replica $ID'
set xrange [0:$maxtime]
set multiplot layout 2,2 columnsfirst
set style line 1 lc 'grey' dt solid lt 1 lw 1
set style line 2 lc 'grey' dt '.' lt 1 lw 1
set grid xtics ytics mxtics mytics ls 1, ls 2
set grid front
set xtics offset 0,0.5
set ytics offset 0.5,0
set mxtics
set mytics
set bmargin 0
set rmargin 0
# set style fill noborder
set key maxrows 2
"            

#  ×·
#  ··

echo "set yrange [-1:1]"
echo "set title 'Total bandwidth' offset 0,-1"
echo "plot '-' with filledcurves lt 1 fs transparent solid 0.2 title 'down (iptables)',"\
           "'' with filledcurves lt 2 fs transparent solid 0.2 title '  up (iptables)',"\
           "'' with steps        lt 1                          title 'down (interface)',"\
           "'' with steps        lt 2                          title '  up (interface)'"
getRes tDown | $TOOLS/movingWindow.pl $SMOOTH
getRes -tUp  | $TOOLS/movingWindow.pl $SMOOTH
sqlite3 -separator ' ' $IN "select time-1, printf(\"%.10f\", round(down/10/1000/1000/1000*8,9)) from net where down*1!=0 and id=$ID;"
echo e
sqlite3 -separator ' ' $IN "select time-1, printf(\"%.10f\", round( -up/10/1000/1000/1000*8,9)) from net where   up*1!=0 and id=$ID;"
echo e

#  ··
#  ×·

echo "set yrange [0:$rpsY]"
echo "set title 'SMR throughput - kRPS' offset 0,-1"
echo "plot '-' with filledcurves x1 fs transparent solid 0.2 title 'kRPS', '-' with lines lt 1 notitle"
sqlite3 -separator ' ' $IN "select time, rps/1000. from rps where id=$ID;" | $TOOLS/movingWindow.pl $SMOOTH | awk '{if($1-prev>0.2) print ""; prev=$1; print}'
echo e
sqlite3 -separator ' ' $IN "select time, rps/1000. from rps where id=$ID;" | $TOOLS/movingWindow.pl $SMOOTH | awk '{if($1-prev>0.2) print ""; prev=$1; print}'
echo e
#  ·×
#  ··

echo "set yrange [-1:1]"
echo "set title 'Bandwidth to other replicas' offset 0,-1"
echo "plot '-' with filledcurves fs transparent solid 0.2 title 'down', '' with filledcurves fs transparent solid 0.2 title 'up'"
getRes rDown | $TOOLS/movingWindow.pl $SMOOTH
getRes -rUp  | $TOOLS/movingWindow.pl $SMOOTH

#  ··
#  ·×

echo "set yrange [-1:1]"
echo "set title 'Bandwidth to clients' offset 0,-1"
echo "plot '-' with filledcurves fs transparent solid 0.2 title 'down', '' with filledcurves fs transparent solid 0.2 title 'up'"
getRes cDown | $TOOLS/movingWindow.pl $SMOOTH
getRes -cUp  | $TOOLS/movingWindow.pl $SMOOTH

echo "unset multiplot"
) | gnuplot -p

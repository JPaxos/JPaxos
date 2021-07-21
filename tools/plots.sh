#!/bin/bash
ID=${1:-0}
IN=${2:-jpdb.sqlite3}
SMOOTH=${3:-0.2}

TOOLS=$(readlink -m $0)
TOOLS=${TOOLS%/*}

count=$(sqlite3 $IN 'select count(*) from dps where id='$ID';')

# Y-range is 5/4 of 95th percentile
dpsY=$(bc <<< "5*$(sqlite3 $IN 'select dps from dps where id='$ID' order by dps desc limit '$((count/20))';'| tail -n1)/4")
rpsY=$(bc <<< "5*$(sqlite3 $IN 'select rps from rps where id='$ID' order by rps desc limit '$((count/20))';'| tail -n1)/4/1000")

maxtime=$(sqlite3 $IN 'select max(time) from rps where id='$ID';')

(
if [[ "$SVG" ]]
then
    echo "
    set term svg size 900,600 dynamic enhanced mouse standalone
    set output \"$SVG\"
    "
else
    echo "set terminal x11 title '${TITLE}$IN - replica $ID'"
fi

echo \
"
set xrange [0:$maxtime]
set multiplot layout 2,2 rowsfirst
set style line 1 lc 'grey' dt solid lt 1 lw 1
set style line 2 lc 'grey' dt '.' lt 1 lw 1
set grid xtics ytics mxtics mytics ls 1, ls 2
set mxtics
set mytics
"

#  ×·
#  ··
echo "set yrange [0:$rpsY]"
echo "plot '-' with lines lw 2 title 'kRPS' "
sqlite3 -separator ' ' $IN "select time, rps/1000. from rps where id=$ID;" | $TOOLS/movingWindow.pl $SMOOTH | awk '{if($1-prev>0.2) print ""; prev=$1; print}'
echo e

#  ·×
#  ··
echo "set yrange [0:100]"
echo "plot '-' with lines lw 2 title 'CPU max', '' with lines lw 2 title 'CPU avg'"
sqlite3 -separator ' ' $IN "select time, max from cpu where id=$ID;" | $TOOLS/movingWindow.pl $SMOOTH
echo e
sqlite3 -separator ' ' $IN "select time, avg from cpu where id=$ID;" | $TOOLS/movingWindow.pl $SMOOTH
echo e

#  ··
#  ×·
echo "set yrange [0:$dpsY]"
echo "plot '-' with lines lw 2 title 'DPS'"
sqlite3 -separator ' ' $IN "select time, dps from dps where id=$ID;" | $TOOLS/movingWindow.pl $SMOOTH | awk '{if($1-prev>0.2) print ""; prev=$1; print}'
echo e

#  ··
#  ·×
#echo "set yrange [-1:1]"
#echo "plot '-' with fsteps title 'down', '' with fsteps title 'up'"
#sqlite3 -separator ' ' $IN "select time, printf(\"%.10f\", round(down/10/1000/1000/1000*8,9)) from net where down*1!=0 and id=$ID;"
#echo e
#sqlite3 -separator ' ' $IN "select time, printf(\"%.10f\", round( -up/10/1000/1000/1000*8,9)) from net where   up*1!=0 and id=$ID;"
#echo e

echo "set yrange [-10:10]"
echo "plot '-' with fillsteps fs transparent solid 0.2 title 'down', '' with fillsteps fs transparent solid 0.2 title 'up', '' with steps lt 1 notitle, '' with steps lt 2 notitle"
sqlite3 -separator ' ' $IN "select time-1, printf(\"%.10f\", round(down/1000/1000/1000*8,9)) from net where down*1!=0 and id=$ID;"
echo e
sqlite3 -separator ' ' $IN "select time-1, printf(\"%.10f\", round( -up/1000/1000/1000*8,9)) from net where   up*1!=0 and id=$ID;"
echo e
sqlite3 -separator ' ' $IN "select time-1, printf(\"%.10f\", round(down/1000/1000/1000*8,9)) from net where down*1!=0 and id=$ID;"
echo e
sqlite3 -separator ' ' $IN "select time-1, printf(\"%.10f\", round( -up/1000/1000/1000*8,9)) from net where   up*1!=0 and id=$ID;"
echo e



echo "unset multiplot"

) | gnuplot -p

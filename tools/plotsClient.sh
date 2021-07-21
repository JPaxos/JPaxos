#!/bin/bash
IN=${1:-jpdb.sqlite3}
SMOOTH=${2:-0.2}

TOOLS=$(readlink -m $0)
TOOLS=${TOOLS%/*}

count=$(sqlite3 $IN "select count(distinct id) from cliCpu ${SELECTOR:+where} ${SELECTOR};")

maxtime=$(sqlite3 $IN 'select max(time) from rps;')

(

if [[ "$SVG" ]]
then
    echo "
    set term svg size 900,600 dynamic enhanced mouse standalone
    set output \"$SVG\"
    "
else
    echo "set terminal x11 title '${TITLE}$IN - clients'"
fi

echo \
"
set xrange [0:$maxtime]
set multiplot layout 2,$count columnsfirst
set style line 1 lc 'grey' dt solid lt 1 lw 1
set style line 2 lc 'grey' dt '.' lt 1 lw 1
set grid xtics ytics mxtics mytics ls 1, ls 2
set mxtics
set mytics
set xtics offset 0,0.5
set ytics offset 0.5,0

set rmargin 0
"

for name in $(sqlite3 $IN "select distinct id from cliCpu ${SELECTOR:+where} ${SELECTOR} order by id asc;")
do

#  ×
#  ·

echo "set tmargin 1"
echo "set bmargin 0"
echo "set yrange [0:100]"
echo "set xtics format '%h'"
echo "set title 'CPU on client $name' offset 0,-1"
echo "plot '-' with lines lw 2 title 'CPU max @ $name', '' with lines lw 2 title 'CPU avg @ $name'"
sqlite3 -separator ' ' $IN "select time, max from cliCpu where id='$name';" | $TOOLS/movingWindow.pl $SMOOTH
echo e
sqlite3 -separator ' ' $IN "select time, avg from cliCpu where id='$name';" | $TOOLS/movingWindow.pl $SMOOTH
echo e

#  ·
#  ×

echo "set tmargin 1"
echo "set bmargin 1"
echo "set yrange [-10:10]"
echo "set notitle"
echo "set notitle"
echo "set xtics format ''"
echo "set xlabel 'Network on client $name' offset 0,1"
echo "plot '-' with fillsteps fs transparent solid 0.2 title 'down', '' with fillsteps fs transparent solid 0.2 title 'up', '' with steps lt 1 notitle, '' with steps lt 2 notitle"
sqlite3 -separator ' ' $IN "select time-0.1, printf(\"%.10f\", round(down/1000/1000/1000*8,9)) from cliNet where down*1!=0 and id='$name';"
echo e
sqlite3 -separator ' ' $IN "select time-0.1, printf(\"%.10f\", round( -up/1000/1000/1000*8,9)) from cliNet where   up*1!=0 and id='$name';"
echo e
sqlite3 -separator ' ' $IN "select time-0.1, printf(\"%.10f\", round(down/1000/1000/1000*8,9)) from cliNet where down*1!=0 and id='$name';"
echo e
sqlite3 -separator ' ' $IN "select time-0.1, printf(\"%.10f\", round( -up/1000/1000/1000*8,9)) from cliNet where   up*1!=0 and id='$name';"
echo e
echo "unset xlabel"

set lmargin 1

echo "set ytics format ''"



done


echo "unset multiplot"

) | gnuplot -p

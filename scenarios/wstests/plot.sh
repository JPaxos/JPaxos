#!/bin/sh
IN1=$(./test1_aggRes.sh test1-20210507_1631.out | awk 'NR!=1 {print $2/1024 " " $5 }')
IN2=$(./test1_aggRes.sh test1-20210510_1027.out | awk 'NR!=1 {print $2/1024 " " $5 }')
IN3=$(./test1_aggRes.sh test1-20210601_1308.out | awk 'NR!=1 {print $2/1024 " " $5 }')

(
echo '
	set output "plot.svg"
	set term svg noenhanced size 880, 500 dynamic lw 0 font ",20" mouse standalone
	set key bottom right
	set xrange [0:128+4]
	set xlabel "Average real batch size [kB]"
	set xtics 16
	set grid
	set yrange [0:375]
	set ytics 50
	set ylabel "Requests [MB/s]"
	plot "-" title "old (b4ecdc75)" with linespoints pt "◎", "-" title "new (f89c5a3d)" with linespoints pt "⧈", "-" title "backptd (9fb2714e)" with linespoints pt "⟐"
'"$IN1"'
e
'"$IN2"'
e
'"$IN3"'
e
') | gnuplot

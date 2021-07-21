#!gnuplot
set output "plot1.svg"
set term svg noenhanced size 880, 500 dynamic lw 0 font ",20" mouse standalone

set xrange [0:(10240+256)]
set yrange [0:440]

set xtics 1024
set mxtics 2
set xtics format "" offset 0, 0.25
do for [i=1:10:1] {
    set xtics add (sprintf("%dkB",i) (i*1024))
}

set key at 10240,90 bottom samplen 2 spacing 1 maxcols 2 maxrows 6 width -1 height -5

set lmargin 4.75
set tmargin 0
set rmargin 0.25
set bmargin 2

set ytics 100
set mytics 4
set ytics offset 0.75, 0

set grid mytics ytics xtics

set ylabel "Client requests (MByte/s)" offset 3, 0
set xlabel "Request size" offset 0, 1
set style data linespoints 
POINTINTERVAL = 1
plot \
0 title " " lw -1 lc rgbcolor "#00ffFFff", \
0 title " " lw -1 lc rgbcolor "#00ffFFff", \
0 title " " lw -1 lc rgbcolor "#00ffFFff", \
\
"Paxos+SS@pmem"  title "Paxos+SS@pmem"   pointinterval POINTINTERVAL pt "□" lc 3 , \
"Paxos+SS@ssd"   title "Paxos+SS@ssd"    pointinterval POINTINTERVAL pt "◇" lc 6, \
"Paxos+epochs"   title "Paxos+epochs"    pointinterval POINTINTERVAL pt "○" lc 10 , \
\
"pPaxosSM@RAM"   title "pPaxosSM@RAM"    pointinterval POINTINTERVAL pt "+" lc 8 , \
"pPaxosSM@emulp" title "pPaxosSM@emulp"  pointinterval POINTINTERVAL pt "×" lc 1 , \
"pPaxosSM@pmem"  title "pPaxosSM@pmem"   pointinterval POINTINTERVAL pt "🞵" lc 2 , \
\
"pPaxos@RAM"     title "pPaxos@RAM"      pointinterval POINTINTERVAL pt "△" lc 4 , \
"pPaxos@emulp"   title "pPaxos@emulp"    pointinterval POINTINTERVAL pt "▷" lc 9 , \
"pPaxos@pmem"    title "pPaxos@pmem"     pointinterval POINTINTERVAL pt "▽" lc 7



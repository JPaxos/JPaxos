#!gnuplot
set output "plot2.svg"
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

set style fill transparent solid 0.5

set ylabel "Client requests (MByte/s)" offset 3, 0
set xlabel "Average request size" offset 0, 1

plot \
0 title " " lw -1 lc rgbcolor "#00ffFFff", \
0 title " " lw -1 lc rgbcolor "#00ffFFff", \
0 title " " lw -1 lc rgbcolor "#00ffFFff", \
\
"Paxos+SS@pmem"  using 1:($2-$3):($2+$3) with filledcurves title "Paxos+SS@pmem"  lc 3  , \
"Paxos+SS@ssd"   using 1:($2-$3):($2+$3) with filledcurves title "Paxos+SS@ssd"   lc 6  , \
"Paxos+epochs"   using 1:($2-$3):($2+$3) with filledcurves title "Paxos+epochs"   lc 10 , \
\
"pPaxosSM@RAM"   using 1:($2-$3):($2+$3) with filledcurves title "pPaxosSM@RAM"   lc 8  , \
"pPaxosSM@emulp" using 1:($2-$3):($2+$3) with filledcurves title "pPaxosSM@emulp" lc 1  , \
"pPaxosSM@pmem"  using 1:($2-$3):($2+$3) with filledcurves title "pPaxosSM@pmem"  lc 2  , \
\
"pPaxos@RAM"     using 1:($2-$3):($2+$3) with filledcurves title "pPaxos@RAM"     lc 4  , \
"pPaxos@emulp"   using 1:($2-$3):($2+$3) with filledcurves title "pPaxos@emulp"   lc 9  , \
"pPaxos@pmem"    using 1:($2-$3):($2+$3) with filledcurves title "pPaxos@pmem"    lc 7



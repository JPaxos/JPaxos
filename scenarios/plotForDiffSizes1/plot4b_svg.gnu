set output "plot4b.svg"
set term svg noenhanced size 880, 500 dynamic lw 0 font ",20" mouse standalone

set xrange [0:(10240+256)]
set yrange [0:875/2]

set xtics 1024
set mxtics 2
set xtics format "" offset 0, 0.25
do for [i=1:10:1] {
    set xtics add (sprintf("%dkB",i) (i*1024))
}

set key at 10240,90 bottom samplen 2 spacing 1 maxcols 2 maxrows 4 width -1 height -5

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
"Paxos+FullSS" title "Paxos+FullSS"   pointinterval POINTINTERVAL pt "□" lc 3 , \
"Paxos+EpochSS" title "Paxos+EpochSS" pointinterval POINTINTERVAL pt "○" lc 6 , \
"P+pmem@RAM" title "P+pmem@RAM"       pointinterval POINTINTERVAL pt "△" lc 4 , \
"P+pmem@pmem" title "P+pmem@pmem"     pointinterval POINTINTERVAL pt "▽" lc 7 , \
"pmemP@RAM" title "pmemP@RAM"         pointinterval POINTINTERVAL pt "+" lc 8 , \
"pmemP@emulp" title "pmemP@emulp"     pointinterval POINTINTERVAL pt "×" lc 1 , \
"pmemP@pmem" title "pmemP@pmem"       pointinterval POINTINTERVAL pt "🞵" lc 2

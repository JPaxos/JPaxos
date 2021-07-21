set output "plot4a.pdf"
set term pdfcairo noenhanced size 8.8cm, 5cm font ",10"

set xrange [0:(5120+128)]
set yrange [0:875/2]

set xtics 1024
set mxtics 4
set xtics format "" offset 0, 0.25
do for [i=1:10:2] {
    set xtics add (sprintf("%dkB",i) (i*1024))
}

set key bottom samplen 2 spacing 1 maxcols 2 maxrows 4 width -1.1

set lmargin 6
set tmargin 0
set rmargin 0

set ytics 100
set mytics 2
set ytics offset 0.75, 0

set grid xtics ytics mxtics mytics

set ylabel "Client requests (MByte/s)" offset 1.5, 0
set xlabel "Request size" offset 0, 0.5
set style data linespoints 
POINTINTERVAL = 1
plot \
0 title " " lw -1 lc rgbcolor "#00ffFFff", \
"pmemP@RAM" title "pPaxosSM@RAM"        pointinterval POINTINTERVAL pt "+" lc 8 , \
"pmemP@emulp" title "pPaxosSM@emulp"    pointinterval POINTINTERVAL pt "×" lc 1 , \
"pmemP@pmem" title "pPaxosSM@pmem"      pointinterval POINTINTERVAL pt "🞵" lc 2 , \
"Paxos+FullSS" title "Paxos+SS"         pointinterval POINTINTERVAL pt "□" lc 3 , \
"Paxos+EpochSS" title "Paxos+epochs"    pointinterval POINTINTERVAL pt "○" lc 6 , \
"P+pmem@RAM" title "pPaxos@RAM"         pointinterval POINTINTERVAL pt "△" lc 4 , \
"P+pmem@pmem" title "pPaxos@pmem"       pointinterval POINTINTERVAL pt "▽" lc 7



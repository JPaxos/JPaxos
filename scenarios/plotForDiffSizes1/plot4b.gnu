set output "plot4b.pdf"
set term pdfcairo noenhanced size 8.8cm, 5cm font ",10"

set xrange [0:(10240+256)]
set yrange [0:875/2]

set xtics 1024
set mxtics 4
set xtics format "" offset 0, 0.25
do for [i=1:10:2] {
    set xtics add (sprintf("%dkB",i) (i*1024))
}

set key bottom samplen 2 spacing 1 maxcols 2 maxrows 4

set lmargin 6
set tmargin 0
set rmargin 0

set ytics 200
set mytics 2
set ytics offset 0.75, 0

set ylabel "Client requests (MByte/s)" offset 1.5, 0
set xlabel "Request size" offset 0, 0.5
set style data linespoints 
POINTINTERVAL = 2
plot \
"Paxos+FullSS" title "Paxos+FullSS"   pointinterval POINTINTERVAL pt "□" lc 3 , \
"Paxos+EpochSS" title "Paxos+EpochSS" pointinterval POINTINTERVAL pt "○" lc 6 , \
"P+pmem@RAM" title "P+pmem@RAM"       pointinterval POINTINTERVAL pt "△" lc 4 , \
"P+pmem@pmem" title "P+pmem@pmem"     pointinterval POINTINTERVAL pt "▽" lc 7 , \
"pmemP@RAM" title "pmemP@RAM"         pointinterval POINTINTERVAL pt "+" lc 8 , \
"pmemP@emulp" title "pmemP@emulp"     pointinterval POINTINTERVAL pt "×" lc 1 , \
"pmemP@pmem" title "pmemP@pmem"       pointinterval POINTINTERVAL pt "🞵" lc 2

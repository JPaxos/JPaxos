set output "plot4.pdf"
set term pdfcairo noenhanced size 8.8cm, 5cm font ",10"

set xrange [0:5256]
set yrange [0:875]

set xtics 1024
set mxtics 4
set xtics format "" offset 0, 0.25
do for [i=1:5:1] {
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
plot 0 title " " lw -1 lc rgbcolor "#00ffFFff", \
"Paxos+FullSS" title "Paxos+FullSS"   pointinterval POINTINTERVAL pt "□" , \
"Paxos+EpochSS" title "Paxos+EpochSS" pointinterval POINTINTERVAL pt "○" , \
"P+pmem@RAM" title "P+pmem@RAM"       pointinterval POINTINTERVAL pt "△" , \
"P+pmem@pmem" title "P+pmem@pmem"     pointinterval POINTINTERVAL pt "▽" lc 1 , \
"pmemP@RAM" title "pmemP@RAM"         pointinterval POINTINTERVAL pt "+" , \
"pmemP@emulp" title "pmemP@emulp"     pointinterval POINTINTERVAL pt "×" , \
"pmemP@pmem" title "pmemP@pmem"       pointinterval POINTINTERVAL pt "🞵"

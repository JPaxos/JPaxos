echo '\documentclass[a4paper,11pt,notitlepage]{article}
\usepackage[pdftex]{graphicx}
\usepackage{multicol}
\usepackage{fullpage}
\begin{document}
'
for sat in net cpu
do
  for cm in FullSS ViewSS EpochSS
  do
    for fl in fc lc
    do
      echo '\begin{table}'
      echo '\vspace{-2em}'
      echo '\begin{tabular}{cc}\hline'
      echo -n "\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.l}.pdf}&"
      echo "\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.f}.pdf}"'\\'    
      echo "\bfseries \Large $sat -- $cm -- $fl&\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.c}.pdf}"'\\'
      echo '\hline'
      echo '\end{tabular}'
      echo '\end{table}'
    done
  done
done
echo '\end{document}'

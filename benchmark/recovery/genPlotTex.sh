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
      echo '\begin{tabular}{cc}'
      echo -n "\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.0}.pdf}&"
      echo "\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.2}.pdf}"'\\'
      echo -n "\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.l}.pdf}&"
      echo "\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.f}.pdf}"'\\'    
      echo "\multicolumn{2}{c}{\includegraphics[width=0.49\textwidth]{{$sat.$cm.$fl.1}.pdf}}"'\\'
      echo '\end{tabular}'
      echo "\caption{$sat -- $cm -- $fl}"
      echo '\end{table}'
    done
  done
done
echo '\end{document}'
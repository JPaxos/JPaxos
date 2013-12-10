#!/bin/bash

beginPic(){
  echo '\begin{tikzpicture}[y=5em,x=0.04em]
 \pgfsetplotmarksize{3pt}
 '
}

endPic(){
  echo '\end{tikzpicture}
 
 '
}

i=0
last=
echo \
'net FullSS  0   0   0   256,02  355,27  773,41  232,29
net ViewSS---fc 41,04   65,54   85,03   95,84   196,25  487,37  461,29
net ViewSS---lc 41,39   55,96   66,32   98,98   189,7   443,67  422,18
net EpochSS 41,64   57,51   70,66   104,34  197,36  481,55  451,41
cpu FullSS  0   0   0   214,7   297,07  525,71  201,78
cpu ViewSS---fc 41,13   61,83   67,79   84,25   189 331,35  345,65
cpu ViewSS---lc 41,39   49,21   51,73   83,81   186,91  332,83  347,47
cpu EpochSS 41,25   50,39   55,61   87,48   192,83  336,54  350,15
null    FullSS---out-of-date    0   0   0   382,75  453,26  655,13  222,7
null    FullSS---up-to-date 0   0   0   0   0   0   220,91
null    ViewSS---fc 40,75   49,29   50,56   83,07   149,49  313,8   333,78
null    ViewSS---lc 40,9    47  50,02   86,24   154,3   305,58  322,92
null    EpochSS 40,92   45,46   48,08   83,6    148,09  304,54  321,48'\
| tr ',' '.' | while read plot model recSent recRcvd recAnsRcvd query snapshot lastRA end
do
let i++

if [[ "$last" != "$plot" ]]
then
  test -z "$last" || endPic
  beginPic
  last="$plot"
fi

echo \
" % $plot - $model
 
 \draw (0,$i) -- (800,$i);

 \draw plot[mark=|,mark size=5pt]         coordinates {(0,$i)} node[below right]{$model} ; % start

 \draw plot[mark=o,mark size=2.5pt]       coordinates {($recSent,$i)} node[above]{} ; % recSent
 \draw plot[mark=triangle]                coordinates {($recRcvd,$i)} node[above]{} ; % recRcvd
 \draw plot[mark=diamond]                 coordinates {($recAnsRcvd,$i)} node[above]{} ; % recAnsRcvd

 \draw plot[mark=*,mark size=2.5pt]       coordinates {($query,$i)} node[above]{} ; % query
 \draw plot[mark=triangle*]               coordinates {($snapshot,$i)} node[above]{} ; % snapshot
 \draw plot[mark=diamond*]                coordinates {($lastRA,$i)} node[above]{} ; % lastRA

 \draw[thick] plot[mark=|,mark size=5pt]  coordinates {($end,$i)} node[above]{} ; % end

"
done

endPic
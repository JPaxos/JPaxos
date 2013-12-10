#!/bin/bash
plotProg=/home/jasiu/TEMP/build-plot-Desktop-Debug/plot

PREFIX=
#PREFIX=D

if [[ -d plots${PREFIX} ]]
then
  echo "plots dir exists"
  exit 1
fi

mkdir -p plots${PREFIX}
#mkdir -p links

for sat in net cpu
do
  for cm in FullSS ViewSS EpochSS
  do
    for fl in fc lc
    do
      for type in l f c
      do
        name="$cm, saturated ${sat}, $( [[ $fl == "fc" ]] && echo -n follower || echo -n leader ) crash, $(case $type in
           0) echo -n replica 0 ;; 
           1) echo -n replica 1;;
           2) echo -n replica 2;;
           l) echo -n leader replica;;
           f) echo -n follower replica;;
           c) echo -n crashed replica;;
          esac
          )"
        ln -s "1${PREFIX}_${sat}.out/$cm/$fl/${type}_avg" "$name"
        #ln -s "../1_${sat}.out/$cm/$fl/${type}_avg" "links/$sat.$cm.$fl.$type.pdf"
        $plotProg "$name"
        mv out.pdf plots${PREFIX}/$sat.$cm.$fl.$type.pdf
        rm "$name"
        echo "$name"
      done
    done
  done
done

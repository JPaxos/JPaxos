#!/bin/bash
SCRIPTDIR=$(realpath $0)
SCRIPTDIR=${SCRIPTDIR%/*}


(
    if [ "$SVG" ]
    then
        echo "
            set term svg size 900,600 dynamic enhanced mouse standalone
            set output \"$SVG\"
            set title \"${3:-$2 @ $1}\" offset 0,-1 noenhanced
            set tmargin 1
        "
    else
        echo "
            set term x11
            set terminal x11 title \"${3:-$2 @ $1}\"
        "
    fi

    echo "
        set cbrange [${ZMIN}:${ZMAX}]
		$EXTRA_GNUPLOT_CMDS
        plot '-' matrix rowheaders columnheaders with image ${SVG:+pixels}
    "
    $SCRIPTDIR/getMatrix.pl $1 $2
    echo e;
) | gnuplot -geometry 1920x1000 -p

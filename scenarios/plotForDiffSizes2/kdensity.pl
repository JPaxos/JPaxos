#!/usr/bin/env perl 

use 5.32.0;
use strict;
use warnings;
use List::Util qw(sum);
use Data::Dumper;

`mkdir -p kdensity`;

sub stdev (\@){
    my $data = shift;
    my $avg = sum(@$data)/@$data;
    my @sqdevs = map { ($_-$avg)**2 } @$data;
    return (sum(@sqdevs)/(@$data-1))**0.5;
}

for my $model (("fullss", "disk", "epochss", "ram", "fakepmem", "pmem")) {
    my $out=`NORM=1 ALL=1 ./getSelected.pl $model l_rps out_20210616_1538`;
    for my $line (split "\n", $out) {
        my ($reqSize, $values) = split ' ', $line;
        
        my @v = split ',', $values;
        
        @v = map { $_/1024/1024 } @v;
        
        my $input='';
        
        $input = "$_ 1\n".$input for (@v);
        
        my $average = sprintf '%.2f',  sum(@v)/@v;
        my $stdev = stdev(@v);
        my $devPercent = sprintf '%.1f%%', 100.*$stdev/(sum(@v)/@v);
        shift @v; shift @v; pop @v; pop @v;
        my $bandwidth = ($v[0]-$v[$#v])/20;
        my $bandwidth2 = ($v[0]-$v[$#v])/10;
        my $average2 = sprintf '%.2f', sum(@v)/@v;
        
        my $median = sprintf '%.2f', ($v[@v/2]+$v[(@v-1)/2])/2;
        
        my $filename = sprintf "%s_%05d", $model, $reqSize;
        
        my $bw  = sprintf '%.2f', $bandwidth;
        my $bw2 = sprintf '%.2f', $bandwidth2;
        
        my $out=`gnuplot 2>&1 << EOF
            set term pngcairo dashed size 960,600 font "DejaVu Sans"
            set output 'kdensity/$filename.png'
            set title "$model $reqSize (stdev: $devPercent; average: $average; average2: $average2; median: $median)"
            set xlabel "requests MB/s"
            set ylabel "kdensity"
            set arrow 5 from $average2, graph 0 to  $average2, graph 1
            set arrow 6 from $median,   graph 0 to  $median,   graph 1
            set arrow 5 nohead dt ". . " lc "#808080" lw 2
            set arrow 6 nohead dt ". . " lc "#808080" lw 2
            set label "MED " at $median,   graph 1  rotate by  90 textcolor "#808080" right
            set label "AVG " at $average2, graph 1  rotate by  90 textcolor "#808080" right
            plot '-' title "bw $bw" smooth kdensity bandwidth $bandwidth, '-' notitle with points pt '⊙', '-' title "bw $bw2" smooth kdensity bandwidth $bandwidth2 lw 2
            $input
            e
            $input
            e
            $input
            e
EOF`;
        print "=== Fail: $model $reqSize ===\n$out\n" if (length $out);
    }
}


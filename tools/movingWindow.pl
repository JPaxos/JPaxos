#!/usr/bin/env perl

# calculates gaussian moving window average with given standard deviation.

use 5.21.0;
use strict;
use warnings;

my $sigma=$ARGV[0] // 10;
my $tsq = 2*$sigma*$sigma;
sub weight{
    return exp(-($_[0]*$_[0])/$tsq)
}

# read input
my @lines;
push @lines, [split] while (<STDIN>);

# sort input
@lines = sort {$a->[0] <=> $b->[0]} @lines;

my %outs;

# calculate the average
for(my $i=0; $i<@lines; ++$i){
    # show progress if STDERR is a tty
    printf STDERR "\r[%".(length scalar @lines)."d/%d]", $i, scalar @lines if(-t STDERR);
    # skip if this point has already been calculated (occurs when combining multiple runs)
    next if(defined $outs{$lines[$i]->[0]});
    
    my @avgs;
    $avgs[$_-1] = 0 for (1 .. @{$lines[$i]}-1);
    
    my $weights=0;
    my $dist;
    
    my $c=$i;
    # down & inclusive
    while($c >= 0 && (3*$sigma) >= abs($dist=$lines[$i]->[0]-$lines[$c]->[0])){
        my $w = weight $dist;
        $weights += $w;
        $avgs[$_-1] += $w*$lines[$c]->[$_] for (1 .. @{$lines[$i]}-1);
        $c--;
    }
    
    $c=$i+1;
    # up
    while($c < @lines && (3*$sigma) >= abs($dist=$lines[$i]->[0]-$lines[$c]->[0])){
        my $w = weight $dist;
        $weights += $w;
        $avgs[$_-1] += $w*$lines[$c]->[$_] for (1 .. @{$lines[$i]}-1);
        $c++;
    }
    
    $avgs[$_-1] /= $weights for (1 .. @{$lines[$i]}-1);
    
    $outs{$lines[$i]->[0]}=\@avgs;
}
printf STDERR " done!\n" if(-t STDERR);

# print out results
for (sort {$a<=>$b} keys %outs){
    print "$_ " . (join " ", @{$outs{$_}}) . "\n";
}


#!/usr/bin/env perl

use 5.21.0;
use Scalar::Util qw(looks_like_number);
use List::MoreUtils qw(first_index);
use List::Util qw(sum);
use Data::Dumper;
use Env qw (ALL DIFFS NORM AVG AVG2 MEDIAN);
use strict;
use warnings;


if (@ARGV < 1){
    print "Missing arg - column idx\n use: $0 <column idx/name> [filename] | column -t";
    exit 1
}

my $idx = shift @ARGV;
my @lineheaders = split " ", <>;

unless($idx =~ /^\d+$/){
    $idx = first_index { $_ eq $idx } @lineheaders;
}

if ($idx < 2 || $idx > @lineheaders){
    print "Bad column idx; known columns:\n" . (join ' ', @lineheaders) . "\n";
    exit 1
}

my %points;

while(<>){
    my @line = split ' ', $_;
    
    my $currReqSize = $line[0];
    my $currReqCli = $line[1];
    

	 my $pointKey = $currReqSize.' '.$currReqCli; 

    my $point = $points{$pointKey} // [()];
    
    push @$point, $line[$idx] * (defined $NORM ? ($currReqSize+9)/2 : 1) if looks_like_number $line[$idx];

    $points{$pointKey} = $point;
}


my @wanted = ("256 2500",
"512 1600",
"768 1000",
"1024 800",
"1536 600",
"2048 600",
"2560 600",
"3072 400",
"3584 400",
"4096 400",
"5120 400",
"6144 300",
"7168 300",
"8192 300",
"9216 300",
"10240 300",
"11264 300",
"12288 300",
"13312 300",
"14336 300",
"15360 300",
"16384 300",
"17408 300",
"18432 300",
"19456 300",
"20480 300");

# print Dumper(\%points);

for my $w (@wanted){
    next unless (defined $points{$w});
    print "$w";
	 my @sorted = sort {$b <=> $a} @{$points{$w}};
    if ( defined $ALL ) {
        print " " . (join ",", @sorted);
    } elsif ( defined $DIFFS ) {
        my $avg = (sum @sorted)/@sorted;
        print " " . $avg . "(" . ($sorted[0] - $sorted[$#sorted]) . ")";
    } elsif ( defined $AVG ) {
		my $avg = (sum @sorted)/@sorted;
		print " " . $avg;
    } elsif ( defined $AVG2 ) {
      shift @sorted;
      shift @sorted;
      pop @sorted;
      pop @sorted;
		my $avg2 = (sum @sorted)/@sorted;
		print " " . $avg2;
    } elsif ( defined $MEDIAN ) {
		my $median = ($sorted[(@sorted-1)/2]+$sorted[@sorted/2])/2;
		print " " . $median;
	} else  {
       my $max = $sorted[0];
  	    print " " . $max;
    }
    print "\n";
}

#!/usr/bin/env perl

use 5.21.0;
use Scalar::Util qw(looks_like_number);
use List::MoreUtils qw(first_index);
use List::Util qw(sum);
use Data::Dumper;
use Env qw (ALL DIFFS NORM AVG AVG2 MEDIAN STDEV MAXN);
use strict;
use warnings;

sub stdev (\@){
    my $data = shift;
    my $avg = sum(@$data)/@$data;
    my @sqdevs = map { ($_-$avg)**2 } @$data;
    return (sum(@sqdevs)/(@$data-1))**0.5;
}

die "Missing arg - column idx\n use: $0 <model> <column idx/name> [filename] | column -t\n" if (@ARGV < 1);

my $model = shift @ARGV;
my @idx   = split ',', shift @ARGV;
my @lineheaders = split " ", <>;

@idx = map {
        my $i = $_;
        return $i if($i =~ /^\d+$/);
        first_index { $_ eq $i } @lineheaders;
    } @idx;

map { die "Bad column idx $_; known columns:\n" . (join ' ', @lineheaders) . "\n" if ($_ < 2 || $_ > @lineheaders); } @idx;

my $ReqSizeIdx = first_index { $_ eq 'ReqSize' } @lineheaders;
my   $ModelIdx = first_index { $_ eq 'Model'   } @lineheaders;

my %points;

my %allRS;

while(<>){
    my @line = split ' ', $_;
    
    my $currReqSize = $line[$ReqSizeIdx];
    my $currModel = $line[$ModelIdx];

    $allRS{$currReqSize} = 1;
    
    my $pointKey = $currModel.' '.$currReqSize; 

    my $point = $points{$pointKey} // [];

    my @values = map {$line[$_]} @idx;

    if(grep {$_ eq ''} map {looks_like_number $_} @values){
        print STDERR "Ignored line that contains something that does not look like number on index(es) " . (join ',', @idx) . ":\n$_";
        next;
    }
    
    push @$point, sum(@values)/@values * (defined $NORM ? ($currReqSize+9)/2 : 1);

    $points{$pointKey} = $point;
}


my @wantedReqSize = (
      256,
      512,
      768,
     1024,
     1536,
     2048,
     2560,
     3072,
     3584,
     4096,
     5120,
     6144,
     7168,
     8192,
     9216,
    10240,
    11264,
    12288,
    13312,
    14336,
    15360,
    16384,
    17408,
    18432,
    19456,
    20480
);

@wantedReqSize = sort {$a<=>$b} keys %allRS;

for my $w (@wantedReqSize){
    my $mw="$model $w";
    next unless (defined $points{$mw});
    print "$w";
    my @sorted = sort {$b <=> $a} @{$points{$mw}};
    if ( defined $ALL ) {
        print " " . (join ",", @sorted);
    } elsif ( defined $DIFFS ) {
        my $avg = (sum @sorted)/@sorted;
        print " " . $avg . "(" . ($sorted[0] - $sorted[$#sorted]) . ")";
    } elsif ( defined $AVG ) {
        my $avg = (sum @sorted)/@sorted;
        print " " . $avg;
        print " " . stdev @sorted if(defined $STDEV);
    } elsif ( defined $AVG2 ) {
        shift @sorted;
        shift @sorted;
        pop @sorted;
        pop @sorted;
        my $avg2 = (sum @sorted)/@sorted;
        print " " . $avg2;
        print " " . stdev @sorted if(defined $STDEV);
    } elsif ( defined $MEDIAN ) {
        my $median = ($sorted[(@sorted-1)/2]+$sorted[@sorted/2])/2;
        print " " . $median;
        print " " . stdev @sorted if(defined $STDEV);
    } elsif ( defined $MAXN ) {
        my $max3avg = (sum @sorted[0..($MAXN-1)])/$MAXN;
        print " " . $max3avg;
    } else {
        my $max = $sorted[0];
        print " " . $max;
    }
    print "\n";
}

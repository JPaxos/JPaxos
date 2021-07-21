#!/usr/bin/env perl

use 5.21.0;
use Scalar::Util qw(looks_like_number);
use List::MoreUtils qw(first_index);
use List::Util qw(sum);
use Env qw (ALL DIFFS);
use strict;
use warnings;


if (@ARGV < 1){
    print "Missing arg - column idx\n use: ./get.pl <column idx/name> [filename] | column -t\n";
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

my %keys;
my %lines;

while(<>){
    my @line = split ' ', $_;
    
    my $currReq = $lines{$line[0]} // {};
    my $currReqCli = $currReq->{$line[1]} // [()];
    
    push @$currReqCli, $line[$idx] if looks_like_number $line[$idx];
    
    $currReq->{$line[1]}=$currReqCli;
    $lines{$line[0]}=$currReq;
    
    $keys{$line[1]}=1;
}

my @sortedlines = sort {$a <=> $b} keys %lines;
print join ' ', "\\ ", @sortedlines, "\n";

for my $key (sort {$a <=> $b} keys %keys){
    print "$key";
    for(@sortedlines){
        unless (defined $lines{$_}->{$key}){
            print " -";
            next;
        }

        my @sorted = sort {$b <=> $a} @{$lines{$_}->{$key}};
        if ( defined $ALL ) {
            print " " . (join ",", @sorted);
        } elsif ( defined $DIFFS ) {
        		my $avg = (sum @sorted)/@sorted;
	         print " " . $avg . "(" . ($sorted[0] - $sorted[$#sorted]) . ")";
	     } else  {
	     		print " " . $sorted[0];
	     }
    }
    print "\n";
}

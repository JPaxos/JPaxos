#!/usr/bin/env perl
use 5.26.0;
use strict;
use warnings;

use Scalar::Util qw(looks_like_number);
use Data::Dumper;

my %r;
for (<>){
	my @l = split " ";
	
	my $id  = shift @l;
	my $run = shift @l;

	$r{$id." ".$run} = () unless defined $r{$id." ".$run};
	
	push @{$r{$id." ".$run}}, \@l;
}

for my $key (keys %r){
	my @sums;
	my @counts;
	for my $aref (@{$r{$key}}){
		for my $i (0..$#$aref){
			next unless looks_like_number $aref->[$i];
			$sums[$i] += $aref->[$i];
			$counts[$i]++;
		}
	}
	print $key;
	for my $i (0..$#sums){
		if ( !defined $counts[$i] || $counts[$i] == 0){
         printf ' -';
		} else {
		    printf  ' %.3f', $sums[$i]/$counts[$i];
		}
	}
	print "\n";
}

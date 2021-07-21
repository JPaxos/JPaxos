#!/usr/bin/env perl

# For files such as 
#   header1 header2 ...
#   value1 value2 ...
#   value1 value2 ...
#   ... ... ...
# calculates statistics of each column:
# min, max, median, average, avrage without N outliers

use 5.26.0;
use strict;
use warnings;
use List::Util qw(min max sum);

my @headers = split ' ', <>;
my @values;

for my $idx (0 .. @headers-1){
	$values[$idx] = [] 
}

for(<>){
	my @line = split ' ';
	for my $idx (0 .. @line-1){
			push @{$values[$idx]}, $line[$idx];
			#print $line[$idx];
			#print ' ';
	}
	#print "\n";
}

print join ' ', "/", @headers;
print "\n";

sub printMyLine {
	print $_[0];
	for my $idx (0 .. @headers-1){
		next if @{$values[$idx]} == 0;
	   print " ";
		print &{$_[1]}(@{$values[$idx]});
	}
	print "\n";
}

printMyLine "min", \&min;
printMyLine "max", \&max;
printMyLine "median", sub () { 
	my @sorted = sort @_;
	($sorted[(@_+0)/2] + $sorted[(@_+1)/2])/2
};

printMyLine "avg", sub () { (sum @_) / @_; };

printMyLine "avg-out1", sub () { 
	my @sorted = sort @_;
	shift @sorted for (1 .. 1);
	pop @sorted for (1 .. 1);
	(sum @sorted) / @sorted;
};

printMyLine "avg-out2", sub () { 
	my @sorted = sort @_;
	shift @sorted for (1 .. 2);
	pop @sorted for (1 .. 2);
	(sum @sorted) / @sorted;
};

printMyLine "avg-out5", sub () { 
	my @sorted = sort @_;
	shift @sorted for (1 .. 5);
	pop @sorted for (1 .. 5);
	(sum @sorted) / @sorted;
};

printMyLine "avg-out10", sub () { 
	my @sorted = sort @_;
	shift @sorted for (1 .. 10);
	pop @sorted for (1 .. 10);
	(sum @sorted) / @sorted;
};


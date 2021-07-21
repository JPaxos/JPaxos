#!/usr/bin/env perl

use 5.32.0;
use strict;
use warnings;
use autodie;
use List::Util qw(sum);
use Data::Dumper;

print join ',', split ' ', <>;
print "\n";

my %entries;

# transforms [ ["a", "b", "c", …], ["1", "2", "3", …] into [ ["a", "1"], ["b", "2"], ["c", "3"], … ]
sub merge(\@){
    my $args = shift;
    for(0..@$args-1){
        die if @{$args->[$_]} != @{$args->[0]};
    }
    
    my @result;
    for my $idx (0..@{$args->[0]}-1){
        my @element;
        push @element, $_->[$idx] for(@$args);
        push @result, \@element;
    }
    return @result;
}

while(<>){
    my @fields = split ' ';
    my $key = join ',', @fields[0..4];
    $entries{$key}=[] unless defined $entries{$key};
    my @data = @fields[5..$#fields];
    push @{$entries{$key}}, \@data;
}

for(sort {
            my @aa = split ',',  $a;
            my @bb = split ',',  $b;
            for(0..3){
                my $res = $aa[$_] <=> $bb[$_];
                return $res if($res!=0);
            }
            return $aa[4] cmp $bb[4];
         } keys %entries){
    print "$_,";
    my @data = map { sum(@$_)/@$_} merge @{$entries{$_}};
    print join ',', @data;
    print "\n";
}

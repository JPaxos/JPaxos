#!/usr/bin/env perl

use 5.32.0;
use strict;
use warnings;
use autodie;
use List::Util qw(sum min max);
use List::MoreUtils qw(pairwise);
use Data::Dumper;

my $line = <>;
print "$line";

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
    my @fields = split ',';
    my $key1 = join ',', @fields[2..4];
    my $key2 = join ',', @fields[0..1];
    $entries{$key1}={} unless defined $entries{$key1};
    my @data = @fields[5..$#fields];
    $entries{$key1}{$key2} = \@data;
}

# print Dumper(\%entries);

my %processed;

for my $key1 (keys %entries){
    my @vals = values %{$entries{$key1}};
    my @valsT = merge @vals;
    
    #my @minV = map {min(@$_)} @valsT;
    my @maxV = map {max(@$_)} @valsT;
    #my @range = pairwise {$a-$b} @maxV, @minV;
    

    for my $key2 (keys %{$entries{$key1}}){
        $key1 =~ /^(.*),(.*),.*?$/;
        my $newkey = "$key2,$1,$2,any";
        $processed{$newkey}=[] unless defined $processed{$newkey};
        
        my $reqsize=$1;
        
        #my @step1 = pairwise {$a-$b} @{$entries{$key1}{$key2}}, @minV;
        #my @step2 = pairwise {$b==0 ? 0.5 : $a/$b} @step1, @range;
        
        my @step2 = pairwise {$a/$b * 1024*1024/(($reqsize+9)/2)} @{$entries{$key1}{$key2}}, @maxV;
        
        push @{$processed{$newkey}}, \@step2;
    }
}

# print Dumper(\%processed);
  
for(sort {
            my @aa = split ',',  $a;
            my @bb = split ',',  $b;
            for(0..3){
                my $res = $aa[$_] <=> $bb[$_];
                return $res if($res!=0);
            }
            return $aa[4] cmp $bb[4];
         } keys %processed){
    print "$_,";
    my @data = map { sum(@$_)/@$_ } merge @{$processed{$_}};
    print join ',', @data;
    print "\n";
}

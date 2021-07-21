#!/usr/bin/env perl 

use 5.21.0;
use strict;
use warnings;
use List::MoreUtils 'uniq';

# sorts sqlGetResultLine.sh output columns so that any columns of 
#   l_name  f\d+_name  \d+_name 
# are transformed to
#   name0 name1 name2 …
# and name0 > name1 > name2
# clients are also sorted.
# clients are the columns that do not match /f?\d+|l/


my @headers = split /\s+/ , <>;

my %columnToCategory;
my %categoryColumnCount;


for ( my $idx=2 ; $idx < @headers ; ++$idx){
    #                  /__   __ \ 
    $headers[$idx] =~ /(.*)_(.*)/;
    #                 \    >   /
    #                  \ ~~~~ /
    
    my $id = $1;
    my $name = $2;
    
    my $prefix = ( $id =~ /f?\d+|l/ ? '' : 'c_' );
    
    $columnToCategory{$idx} = $prefix.$name;
    $categoryColumnCount{$prefix.$name} = ($categoryColumnCount{$prefix.$name} // 0) + 1;
}

print "reqsize clicount";
for my $name (sort keys %categoryColumnCount){
    for (my $i=0; $i<$categoryColumnCount{$name}; ++$i){
        print " $name$i";
    }
}
print "\n";
        
for(<>){
    my @entries = split /\s+/;

    if (@entries != @headers){
        print STDERR "$entries[0] $entries[1] has bad column count!\n";
        next;
    }
    
    print "$entries[0] $entries[1]";
    
    my %categories;

    $categories{$_}=() 
        for uniq sort values %columnToCategory;
    
    for ( my $idx=2 ; $idx < @entries ; ++$idx){
        push @{$categories{$columnToCategory{$idx}}}, $entries[$idx];
    }
    
    for my $name (sort keys %categoryColumnCount){
        for my $res (sort {$b<=>$a} @{$categories{$name}}){
            print " $res";
        }
    }
    print "\n";
}

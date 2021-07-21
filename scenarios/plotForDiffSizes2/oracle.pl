#!/usr/bin/env perl

use 5.32.0;
use strict;
use warnings;
use Scalar::Util qw(looks_like_number);
use experimental qw(switch);

use constant USAGE => "\n $0 {fullss|disk|epochss|ram|fakepmem|pmem} <requestSizeInBytes>\nReturns window size and batching size, for instance:\n 5 65536\n";

die "Bad argument count" . USAGE unless @ARGV==2;

my $model = $ARGV[0];
die "Bad model name" . USAGE unless $model =~ /^fullss|disk|epochss|ram|fakepmem|pmem$/;

my $reqSize = $ARGV[1];
die "Request size does not look like a number" . USAGE unless looks_like_number $reqSize;

my $ws="UNDEFINED";
my $bs="UNDEFINED";

given($model){
    when ("disk"){
        given($reqSize){
            when($_ <=     512){ ($ws,$bs) = (6,  64*1024);}
            when($_ <=  3*1024){ ($ws,$bs) = (5, 256*1024);}    
            default            { ($ws,$bs) = (8, 256*1024);}
        }
    }
    when ("fullss"){
        given($reqSize){
            when($_ <=     256){ ($ws,$bs) = (5,  48*1024);}
            when($_ <=     512){ ($ws,$bs) = (5, 112*1024);}
            when($_ <=    1024){ ($ws,$bs) = (7, 112*1024);}
            when($_ <=  4*1024){ ($ws,$bs) = (7, 160*1024);}
            default            { ($ws,$bs) = (8, 160*1024);}
        }
    }
    when (/^epochss|ram$/){
        given($reqSize){
            when($_ <=    1024){ ($ws,$bs) = (3,  48*1024);}
            when($_ <=  2*1024){ ($ws,$bs) = (5,  48*1024);}
            default            { ($ws,$bs) = (7,  80*1024);}
        }
    }
    when (/^fakepmem|pmem$/){
        given($reqSize){
            when($_ <=    1024){ ($ws,$bs) = (3,  48*1024);} # <=1
            when($_ <=  2*1024){ ($ws,$bs) = (8,  48*1024);} # 1 2
            when($_ <=  7*1024 &&                            
                 $_ >   4*1024){ ($ws,$bs) = (7,  80*1024);} #         5,6,7
            when($_ <  10*1024){ ($ws,$bs) = (8, 112*1024);} #     3,4,      8,9
            default            { ($ws,$bs) = (9, 192*1024);} # >=10
        }
    }
    default {
        die "should never happen";
    }
}

print "$ws $bs\n";

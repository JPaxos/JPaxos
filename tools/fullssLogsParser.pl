#!/usr/bin/env perl

use 5.26.0;
use strict;
use warnings;
use autodie;
use Digest::MD5 qw(md5_hex);
use experimental qw(switch);
use Term::ANSIColor qw(colored);

my @snaps;
my @views;
my @logs;
my @epochs;

die "Usage: $0 <dir>" unless (@ARGV == 1);
my $dir = $ARGV[0];
my $dirH;
opendir $dirH, $dir;
while (readdir $dirH) {
	push @views  , $_ if(/^sync\.\d+\.view/);
	push @logs   , $_ if(/^sync\.\d+\.log/);
	push @epochs , $_ if(/^sync\.epoch\.\d+/);
	push @snaps  , $_ if(/^snapshot\.\d+/);
}
closedir $dirH;

sub mnsort {$a=~/(\d+)/; my $aa=$1; $b=~/(\d+)/; my $bb=$1;; $aa<=>$bb};
@views = sort mnsort @views;
@logs = sort mnsort @logs;
@epochs = sort mnsort @epochs;
@snaps = sort mnsort @snaps;

print "View files: ";
print join ',', map {/(\d+)/; $1} @views;
print "\nLog files: ";
print join ',', map {/(\d+)/; $1} @logs;
print "\nEpoch files: ";
print join ',', map {/(\d+)/; $1} @epochs;
print "\nSnapshot files: ";
print join ',', map {/(\d+)/; $1} @snaps;
print "\n";

print '=' x 80;
foreach my $vf (@views){
	print "\n$vf";
	open my $v, "<:raw", "$dir/$vf";
	my $lastRead = 0;
	my $isEmpty = 1;
	my $rawView;
	$isEmpty = 0, print "\n view changed to " . unpack('L>',$rawView)
		while(4==($lastRead = read $v, $rawView, 4));
	print "\n torn view change to: " . (join ' ', unpack("b8"x$lastRead, $rawView))
		if($lastRead % 4);
	print "\n empty file"
		if($isEmpty);
	close $v;
}

print "\n";
print '=' x 80;

foreach my $ef (@epochs){
	print "\n$ef: ";
   open my $v, "<:raw", "$dir/$ef";
   my $c = read $v, my $e, 8;
   print "epoch " . unpack ('Q>',$e)                     if($c==8);
   print "torn epoch " . (join ' ', unpack ("b8"x$c,$e)) if($c %8);
   print "empty"                                         if($c==0);
   close $v;
}
print "\n";

print '=' x 80;
SNAP:
foreach my $sf (@snaps){
	print "\n$sf:";
	open my $s, "<:raw", "$dir/$sf";
	my $temp;
	if(8 != read $s, $temp, 8){
		print "\n torn snapshot"; close $s; next;
	}
	my ($nextIntanceId, $valueLength) = unpack 'L>'x2, $temp;
	print "\n next instance id: $nextIntanceId";
	print "\n value length    : $valueLength";
   if($valueLength != read $s, $temp, $valueLength){
		print "\n torn snapshot"; close $s; next;
	}
	print "\n value md5       : " . md5_hex($temp);

	if(4 != read $s, $temp, 4){
		print "\n torn snapshot"; close $s; next;
	}
	my $lastReplyForClientSize = unpack 'L>', $temp;
	print "\n last replies for clients: $lastReplyForClientSize\n  ";
	for (1..$lastReplyForClientSize){
		my $expected=8+4+8+4+4;
		if($expected != read $s, $temp, $expected){
			print "\n torn snapshot"; close $s; next SNAP;
		}
		my @a = unpack 'Q>L>Q>L>L>', $temp;
		print "($a[2]:$a[3]), ";
		$expected = $a[4];
		if($expected != read $s, $temp, $expected){
			print "\n torn snapshot"; close $s; next SNAP;
		}
	}

	if(12 != read $s, $temp, 12){
		print "\n torn snapshot"; close $s; next;
	}
	my ($nextRequestSeqNo, $startingRequestSeqNo, $prcSize) = unpack 'L>'x3, $temp;
	print "\n next seq no     : $nextRequestSeqNo";
	print "\n starting seq no : $startingRequestSeqNo";
	print "\n response cahce  : $prcSize";
	for (1..$prcSize){
		my $expected=4+8+4+4;
		if($expected != read $s, $temp, $expected){
			print "\n torn snapshot"; close $s; next SNAP;
		}
		my @a = unpack 'L>Q>L>L>', $temp;
		print "\n   ($a[1]:$a[2])";
		$expected = $a[3];
      if($expected != read $s, $temp, $expected){
			print "\n torn snapshot"; close $s; next SNAP;
		}
	}
	my $c = read $s, $temp, 2**31-1;
	print "\n surplus data past snapshot!"
		unless($c==0);
	close $s;
}

print "\n";
print '=' x 80;
print "\n";

LOG:
foreach my $lf (@logs){
   print "\n$lf:";
   open my $l, "<:raw", "$dir/$lf";
   my $temp;

   RECORD:
   while(1 == read $l, $temp, 1){
		for (unpack 'C', $temp){
			when(0x02) {
				print "\n change value: ";
				my $expected = 4+4+4;
				if($expected != read $l, $temp, $expected){
					print "\n torn log"; close $l; next LOG;
				}
				my ($instance, $view, $valueLen) = unpack 'L>L>l>', $temp;
				print "i:$instance v:$view  length:$valueLen";
				if($valueLen != -1){
					$expected = $valueLen;
	            if($expected != read $l, $temp, $expected){
						print "\n torn log"; close $l; next LOG;
					}
	            print " md5:" . md5_hex($temp) . "  ";

     				my $reqCnt = unpack 'l>', $temp;
     				my $offset = 4;

					for (1..$reqCnt){
						if($expected < $offset + 16){
							print "\n batch torn or longer than length!"; last;
						}
						my ($cliId, $seqId, $reqLen) = unpack '@'.$offset.'q>l>l>', $temp;
						print "($cliId:$seqId) ${reqLen}B ";
						$offset += 16;
						if($expected < $offset + $reqLen){
							print "\n batch torn or longer than length!"; last;
						}
						my $req = unpack '@'.$offset.'a'.$reqLen, $temp;
						print substr(md5_hex($req),0,4) . ", ";
						$offset += $reqLen;
					}

     				if($offset!=$valueLen){
     					print "\n batch contents mismatches its length!"; close $l; next LOG;
     				}
				}
			}
			when(0x03) {
				print "\n snapshot: ";
	         if(4 != read $l, $temp, 4){
				   print "\n torn log"; close $l; next LOG;
				}
            print unpack('L>', $temp);
			}
			when(0x21) {
				print "\n decide: ";
		      if(4 != read $l, $temp, 4){
			   	print "\n torn log"; close $l; next LOG;
				}
            print unpack('L>', $temp);
			}
			default {
				print colored("\n unknown type: $_", 'bright_red');
				my $c = read $l, $temp, 2**31-1;
				print "\n remaining bytes: $c";
				last RECORD;
			}
		}
   }
   close $l;
}

print "\n";
print '=' x 80;
print "\n";

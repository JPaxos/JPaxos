#!/usr/bin/env perl

use 5.26.0;
use strict;
use warnings;

use feature 'say';

use DBI;
use DBD::SQLite::Constants qw/:file_open/;
use Data::Dumper;
use Env;

#https://metacpan.org/pod/DBI

if( defined $ENV{"HEADER"}){
    print "id run";
    printf " recStart";
    printf " Rsent";
    printf " Rrcvd";
    printf " RAsent";
    printf " RArcvd";
    
    printf " RcuBegin";
    printf " cuBegin";

    printf " cuFirstQuerySent-up";
    printf " cuQueryRcvd-up";
    
    printf " cuSnapSent-up";
    printf " cuSnapRcvd-up";
    printf " cuSnapUnpa-up";
    printf " cuRespSent-up";
    printf " cuEnd";
    printf " RcuEnd";
    printf " recEnd";
    printf "\n";
    
    exit unless ( $ENV{"HEADER"} eq "1");
    
}


my $dbpath = $ARGV[0] // "jpdb.sqlite3";

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbpath",undef,undef,{sqlite_open_flags => SQLITE_OPEN_READONLY})
    or die "Could not open database: " . $DBI::errstr;
    
my $cuStartsStmt = $dbh->prepare("select id, run, time from catchupStart order by time asc");
# note to self: perl sqlite acts as if single quotation marks were added around bind parameters unless explictly asked not to do it; sqlite +0 is quicker
my $cuEndStmt = $dbh->prepare("select time from catchupEnd where id = ?+0 and run = ?+0 and time > ?+0                  order by time asc  limit 1");
my $cuFirstQuerySendStmt = $dbh->prepare("select time, rcpt from catchQuerySent where id = ?+0 and run = ?+0 and time >= ?+0 and time <= ?+0 order by time asc limit 1");
my $cuQueryRcvdStmt = $dbh->prepare("select time from catchQueryRcvd where sender = ?+0 and id = ?+0 and time >= ?+0 and time <= ?+0 order by time asc");
my $cuSnapSentStmt = $dbh->prepare("select time from catchSnapSent where rcpt = ?+0 and id = ?+0 and time >= ?+0 and time <= ?+0 order by time asc");
my $cuSnapRcvdStmt = $dbh->prepare("select time from catchSnapRcvd where id = ?+0 and run = ?+0 and time >= ?+0 and time <= ?+0 order by time asc limit 1");
my $cuSnapApplStmt = $dbh->prepare("select time from catchSnapApplied where id = ?+0 and run = ?+0 and time >= ?+0 and time <= ?+0 order by time asc limit 1");
my $cuRespSentStmt = $dbh->prepare("select time from catchRespSent where rcpt = ?+0 and id = ?+0 and time >= ?+0 and time <= ?+0 order by time asc");
my $cuRespRcvdStmt = $dbh->prepare("select time from catchRespRcvd where id = ?+0 and run = ?+0 and time >= ?+0 and time <= ?+0 order by time asc limit 1");

my $recStartStmt = $dbh->prepare("select time from recoverystart where id = ?+0 and run = ?+0");
my $recEndStmt = $dbh->prepare("select time from recoveryend where id = ?+0 and run = ?+0");

my $recCUStartStmt = $dbh->prepare("select time from reccatchupstart where id = ?+0 and run = ?+0");
my $recCUEndStmt = $dbh->prepare("select time from reccatchupend where id = ?+0 and run = ?+0");

my $recSentStmt = $dbh->prepare("select time from recsent");
my $recRcvdStmt = $dbh->prepare("select max(time) from recrcvd");

my $recAckSentStmt = $dbh->prepare("select max(time) from recacksent");
my $recAckRecvdStmt = $dbh->prepare("select max(time) from recackrcvd");



my $upStmt = $dbh->prepare("select time from start where id = ?+0 and run = ?+0 and time <= ?+0 order by time asc limit 1");

$cuStartsStmt->execute();
my $cuStarts = $cuStartsStmt->fetchall_arrayref;

foreach (@$cuStarts){
    my ($id, $run, $starttime) = @$_;

    $upStmt->execute($id, $run, $starttime);
    my ($up) = $upStmt->fetchrow_array;
    
    # check when it edned
    $cuEndStmt->execute($id, $run, $starttime);
    my ($endtime) = $cuEndStmt->fetchrow_array;
    
    # common bind parameters
    my @wheresMe = ($id, $run, $starttime, $endtime);
    
    $cuFirstQuerySendStmt->execute(@wheresMe);
    my ($cuFirstQuerySent, $cuQueryRcpt) = $cuFirstQuerySendStmt->fetchrow_array;
    
    my @wheresPeer = ($id, $cuQueryRcpt, $starttime, $endtime);
    
    $cuQueryRcvdStmt->execute(@wheresPeer);
    my ($cuQueryRcvd) = $cuQueryRcvdStmt->fetchrow_array;
    
    $cuSnapSentStmt->execute(@wheresPeer);
    my ($cuSnapSent) = $cuSnapSentStmt->fetchrow_array;
    $cuSnapRcvdStmt->execute(@wheresMe);
    my ($cuSnapRcvd) = $cuSnapRcvdStmt->fetchrow_array;
    $cuSnapApplStmt->execute(@wheresMe);
    my ($cuSnapAppl) = $cuSnapApplStmt->fetchrow_array;
    
    $cuRespSentStmt->execute(@wheresPeer);
    my ($cuRespSent) = $cuRespSentStmt->fetchrow_array;
    $cuRespRcvdStmt->execute(@wheresMe);
    my ($cuRespRcvd) = $cuRespRcvdStmt->fetchrow_array;



    my @wheresRec = ($id, $run);

    $recStartStmt->execute(@wheresRec);
    my ($recStart) = $recStartStmt->fetchrow_array;
    $recEndStmt->execute(@wheresRec);
    my ($recEnd) = $recEndStmt->fetchrow_array;

    $recCUStartStmt->execute(@wheresRec);
    my ($recCUStart) = $recCUStartStmt->fetchrow_array;
    $recCUEndStmt->execute(@wheresRec);
    my ($recCUEnd) = $recCUEndStmt->fetchrow_array;


    $recSentStmt->execute();
    my ($recSent) = $recSentStmt->fetchrow_array;
    $recRcvdStmt->execute();
    my ($recRcvd) = $recRcvdStmt->fetchrow_array;

    $recAckSentStmt->execute();
    my ($recAckSent) = $recAckSentStmt->fetchrow_array;
    $recAckRecvdStmt->execute();
    my ($recAckRecvd) = $recAckRecvdStmt->fetchrow_array;



    
    
    # print "$id | $run | U: $up | S: $starttime | Q→ $cuFirstQuerySent ($cuQueryRcpt) | →Q $cuQueryRcvd | S→ $cuSnapSent | →S $cuSnapRcvd | S! $cuSnapAppl | R→ $cuRespSent | →R $cuRespRcvd | E: $endtime | \n";
    
    print "$id $run";
    if (defined $recStart)    { printf " %f" , $recStart-$up; }     else { printf " -";}

    if (defined $recSent)     { printf " %f" , $recSent-$up; }      else { printf " -";}
    if (defined $recRcvd)     { printf " %f" , $recRcvd-$up; }      else { printf " -";}
    if (defined $recAckSent)  { printf " %f" , $recAckSent-$up; }   else { printf " -";}
    if (defined $recAckRecvd) { printf " %f" , $recAckRecvd-$up; }  else { printf " -";}
    
    if (defined $recCUStart)  { printf " %f" , $recCUStart-$up; }   else { printf " -";}
    if (defined $starttime)   { printf " %f" , $starttime-$up; }    else { printf " -";}
    
    if (defined $cuFirstQuerySent) { printf " %f" , $cuFirstQuerySent-$up; } else { printf " -";}
    if (defined $cuQueryRcvd) { printf " %f" , $cuQueryRcvd-$up; }  else { printf " -";}
    

    if (defined $cuSnapSent) { printf " %f" , $cuSnapSent-$up; }    else { printf " -";}
    if (defined $cuSnapRcvd) { printf " %f" , $cuSnapRcvd-$up; }    else { printf " -";}
    if (defined $cuSnapAppl) { printf " %f" , $cuSnapAppl-$up; }    else { printf " -";}
    
    if (defined $cuRespSent) { printf " %f" , $cuRespSent-$up; }    else { printf " -";}
    if (defined $endtime)    { printf " %f" , $endtime-$up; }       else { printf " -";}
    if (defined $recCUEnd)   { printf " %f" , $recCUEnd-$up; }      else { printf " -";}
    if (defined $recEnd)     { printf " %f" , $recEnd-$up; }        else { printf " -";}
    
    printf "\n";
    
}

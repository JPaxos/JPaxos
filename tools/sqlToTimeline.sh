#!/bin/bash

DB=${1:-jpdb.sqlite3}

(
sqlite3 $DB '
-- order of these statements determines the order in which events of equal time appear
select printf("%d %06.3f ===UP===",                     id, time)                from up;
select printf("%d %06.3f Start",                        id, time)                from start;
select printf("%d %06.3f Recovering...",                id, time)                from recoveryStart;

select printf("%d %06.3f sent Recovery(%d)",            id, time, msgid)         from recSent;
select printf("%d %06.3f sent RecAck(%d) to %d",        id, time, msgid, rcpt)   from recAckSent;
select printf("%d %06.3f got Recovery(%d) from %d",     id, time, msgid, sender) from recRcvd;
select printf("%d %06.3f got RecAck(%d) from %d",       id, time, msgid, sender) from recAckRcvd;

select printf("%d %06.3f sent Prep(%d)",                id, time, view)          from prepSent;
select printf("%d %06.3f got Prep(%d)",                 id, time, view)          from prepRcvd;

select printf("%d %06.3f view advanced to %d",          id, time, view)          from viewChanged;

select printf("%d %06.3f sent PrepOk(%d)",              id, time, view)          from prepOkSent;
select printf("%d %06.3f got PrepOk(%d) from %d",       id, time, view, sender)  from prepOkRcvd;

select printf("%d %06.3f I am now leader of %d",        id, time, view)          from viewchangeSucceeded;


select printf("%d %06.3f Recovery forced catchup",      id, time)                from recCatchupStart;
select printf("%d %06.3f Catch up triggered",           id, time)                from catchupStart;

select printf("%d %06.3f sent CatchUpQuery to %d",      id, time, rcpt)          from catchQuerySent;
select printf("%d %06.3f sent CU snapshot to %d",       id, time, rcpt)          from catchSnapSent;
select printf("%d %06.3f streaming CU instances to %d", id, time, rcpt)          from catchRespSent;

select printf("%d %06.3f got CatchUpQuery from %d",     id, time, sender)        from catchQueryRcvd;
select printf("%d %06.3f got CU snapshot from %d",      id, time, sender)        from catchSnapRcvd;
select printf("%d %06.3f applied CU snapshot",          id, time)                from catchSnapRcvd;
--select printf("%d %06.3f got CU instances from %d",     id, time, sender)        from catchRespRcvd;

select printf("%d %06.3f Catch up finished",            id, time)                from catchupEnd;
select printf("%d %06.3f Recovery needs CU no more",    id, time)                from recCatchupEnd;
select printf("%d %06.3f Recovered.",                   id, time)                from recoveryEnd;
'

for id in {0..2}
do
    sqlite3 $DB 'select time, id, rcpt from catchquerysent where id='$id';' |\
    perl -e '
    use DBI;
    use DBD::SQLite::Constants qw/:file_open/;
    my $dbh = DBI->connect("dbi:SQLite:dbname='$DB'",undef,undef,{sqlite_open_flags => SQLITE_OPEN_READONLY});
    my @cq;
    push @cq, [split /\|/, $_] for(<>);
    for(my $i=0; $i < @cq; ++$i){
        my $where="where id='$id' and time >= " . @{$cq[$i]}[0];
        $where.=" and time <= @{$cq[$i+1]}[0]" if(defined $cq[$i+1]);

		  my $sth = $dbh->prepare("select count(*) from catchRespRcvd ".$where);
		  $sth->execute();
		  my $count = $sth->fetch->[0];

		  if($count==1){
   	     print "select printf(\"%d %06.3f got (1/1) CU instances from %d\",   id, time, sender)                     from catchRespRcvd ".$where." limit 1;\n";
		  } elsif($count==2){
	        print "select printf(\"%d %06.3f got (1/2) CU instances from %d\",   id, time, sender)                     from catchRespRcvd ".$where." limit 1;\n";
	        print "select printf(\"%d %06.3f got (2/2) CU instances from %d\",   id, time, sender)                     from catchRespRcvd ".$where." order by time desc limit 1;\n";
		  } elsif($count>2){
          print "select printf(\"%d %06.3f got (1/$count) CU instances from %d\",      id, time, sender)                     from catchRespRcvd ".$where." limit 1;\n";
          print "select printf(\"%d %06.3f getting %d × CU instances from %d\",        id, min(time), count(time)-2, sender) from catchRespRcvd ".$where.";\n";
          print "select printf(\"%d %06.3f got ($count/$count) CU instances from %d\", id, time, sender)                     from catchRespRcvd ".$where." order by time desc limit 1;\n";
        }
    }
    '
done | sqlite3 $DB

) \
| perl -e 'my @lines; push @lines, $_ for(<>); use sort "stable"; print $_ for(sort {$a=~/\d ([\d.]*) /; my $aa=$1; $b=~/\d ([\d.]*) /; my $bb=$1; $aa <=> $bb} @lines);' \
| perl -e '
my @last=(0)x3;
my $prev=0;
for(<>){
	/(\d) ([\d.]+) (.*)/;
	my $eI=$2-$last[$1];
	my $e=$2-$prev;
	$last[$1]=$2;
	$prev=$2;
	# timestamp
	printf "%6.3f%3s", $2, "";
	# space
	print " " x (40*$1);
	# time from previous event on any replica
	printf "[\033[31m%+4d\033[0m", $e*1000;
	# time from previous event on this replica
	printf "/\033[%sm%+4d\033[0m]", $1>0?$1>1?"36":"33":"32" , $eI*1000;
	# event
	printf " \033[1m%s\n\033[0m", $3;
}
'


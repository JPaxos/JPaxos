mkdir svgs

export RECOVERY_TIME=$( for DB in lc/*.sqlite3; do sqlite3 $DB "select time from start where run=1;"; done | perl -e 'use List::Util "sum"; my @a; push @a, $_ for(<>); printf "%.3f", sum(@a)/@a;' )
export CRASH_TIME=$( for DB in lc/*.sqlite3; do sqlite3 $DB "select max(time) from rps where id=4 and time < 40"; done | perl -e 'use List::Util "sum"; my @a; push @a, $_ for(<>); printf "%.3f", sum(@a)/@a+0.05;' )
export TITLE="[jpaxos@fullss, 81MB snapshot, leader crash]"
export XMAX=120
export PREFIX='results/lc_'
export TARGET='svgs/lc_'
export R0='follower used for catchUp'
export R1='follower becoming leader'
export R2='crashing leader'
~/jpaxos_pmem/scenarios/recoveryTest/makeSvgs.sh


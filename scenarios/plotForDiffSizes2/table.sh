#!/bin/bash
IN=`echo                      \
    Paxos+epochs              \
    Paxos+SS@{ssd,pmem}       \
    pPaxos@{RAM,emulp,pmem}   \
    pPaxosSM@{RAM,emulp,pmem} \
`

SL=`for F in $IN
    do
        awk '{print $1}' $F
    done | sort -n | uniq
`

echo \
"        Epochs  ┌───FullSS───┐  ┌───────pPaxos───────┐  ┌──────pPaxosSM──────┐
ReqSiz  ------   @disk   @pmem   @RAM   @emulp   @pmem   @RAM   @emulp   @pmem"

for S in $SL
do
    printf "%6s" $S;
    ORDER=
    RES=
    for F in $IN
    do
        R=`awk '$1=="'$S'" {printf "  %6.2f" , $2}' $F`
        RES="$RES$R"
        ORDER=`echo -ne "$ORDER\n$R\t$F"`
    done
    if [[ "$NORM" && $NORM -ge 1 && $NORM -le 9 ]]
    then
        perl -e '
            $_=<>; s/^\s+//;
            my @a = split /\s+/;
            my $n = @a['$((NORM-1))']; # this is pPaxos@pmem
            map {if($_ eq $n){ printf "  >base<"} else {printf "  %5.1f%%", 100*($_/$n-1)}} @a;
            ' <<< "$RES"
        echo;    
    else
        echo -n "$RES"
        echo "$ORDER" | perl -e '
            use Data::Dumper;
            my %r;
            while(<>){
                s/^\s+//; s/\s+$//;
                s/Paxos\+?//;
                s/pmem/pme/;
                s/emulp/emu/;
                next unless(length);
                my ($k,$v) = split /\s+/;
                $r{$k}=$v
            }
            my @o;
            for (sort keys %r){
                push @o, sprintf("%7s", $r{$_});
            }
            print "  ┆ " . (join " < ", @o) . "\n";
            '
    fi
done

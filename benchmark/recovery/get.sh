#!/bin/bash

listSubdirs(){
  find -maxdepth 1 -mindepth 1 -type d
}

getTimeOfEvent(){
  name="$1"
  shift
  grep -h "$name" "$@" | awk '{print $1}'
}

getTimeOfFirstEvent(){
  getTimeOfEvent "$@" | head -n1
}

getTimeOfLastEvent(){
  getTimeOfEvent "$@" | tail -n1
}

getRecStats(){
  started=$(getTimeOfEvent "started" 1.1_shifted)
  finished=$(getTimeOfEvent "finished" 1.1_shifted)
  recoverySent=$(getTimeOfEvent "Sending Recovery" 1.1_shifted) 
  recoveryRcvd="$(getTimeOfEvent "Received Recovery" {0,2}.0_shifted | tr '\n' ',')"
  recoveryAns="$(getTimeOfEvent "Received RecoveryAnswer" 1.1_shifted | tr '\n' ',')"
  cuq="$(getTimeOfFirstEvent "CatchUpQuery"  1.1_shifted)"
  cus="$(getTimeOfFirstEvent "CatchUpSnapshot"  1.1_shifted)"
  culr="$(getTimeOfLastEvent "CatchUpResponse"  1.1_shifted)"
  cuqn="$(getTimeOfEvent "CatchUpQuery" 1.1_shifted | wc -l)"
  curn="$(getTimeOfEvent "CatchUpResponse" 1.1_shifted | wc -l)"
  # awk '!silence;/finished/{silence=1}' 1.1_shifted
  # awk '//;/finished/{exit}' 1.1_shifted
  
  lra="$(getTimeOfLastEvent "Received RecoveryAnswer" 1.1_shifted)"
  
  echo "$started,$finished,$recoverySent,${recoveryRcvd}${recoveryAns}$cuq,$cus,$culr,$cuqn,$curn" > recStats
  
  if [[ "$cm" == FullSS && "$DIR" == "3_all.out" ]]
  then
    # no snapshots exist here...
    return
  fi
  
  if (( lra - started > 3000 ))
  then
    touch badTcp
  fi
  
  if [[ -z "$finished" ]]
  then
    touch badNoRec
  fi
}


doStuffInSubDir(){
  start=$(grep -h 'Recovery phase started' ?.? | sort -n | awk 'NR==1 {print $1}')
  for x in ?.?; do
    awk '{printf("%d", $1-'${start}'); for(i=2;i<=NF;++i) printf(" %s", $i); printf("\n");}' $x | sort -snk1,1 > ${x}_shifted
  done;

  for r in 0 1 2; do
    echo 'time, rps' > ${r}_rps
    for x in ${r}.?_shifted; do
      awk '/RPS/ {printf("%s, %s\n", $1, $3);}' $x >> ${r}_rps
    done;
  done;
  
  getRecStats
}

doStuffInMainDir(){
  analyze0=
  analyze1=
  analyze2=
  echo "tno,started,finished,recoverySent,recoveryRcvd0,recoveryRcvd2,recoveryAns,recoveryAns,firstCatchUpQuery,firstCatchUpSnapshot,lastCatchUpResponse,CatchUpQueryCnt,CatchUpResponseCnt" > recStats
  for d in "$@"
  do
    pushd $d
    doStuffInSubDir
    
    if [[ -e badTcp || -e badNoRec ]]
    then
      popd
      continue
    fi
    
    analyze0="${analyze0} ${d}/0_rps"
    analyze1="${analyze1} ${d}/1_rps"
    analyze2="${analyze2} ${d}/2_rps"
    echo -ne "$d," >> ../recStats
    cat recStats >> ../recStats
    popd
  done

  java -cp "$base"/averageFromRanges.jar averageFromRanges $analyze0 > 0_avg
  java -cp "$base"/averageFromRanges.jar averageFromRanges $analyze1 > 1_avg
  java -cp "$base"/averageFromRanges.jar averageFromRanges $analyze2 > 2_avg
  
  "$base"/exclude_outliers.sh
}

# @Unused
detectLC() {
  for x in {1..25}; do pushd $x > /dev/null; grep 'Suspecting [^0]' * > /dev/null && echo $x; popd > /dev/null; done
}

# @Unused
detectFC() {
  for x in {1..25}; do pushd $x > /dev/null; grep 'Suspecting [^0]' * > /dev/null || echo $x; popd > /dev/null; done
}

base=$(readlink -e $0)
base=${base%/*}

if [[ ! -d "$1" ]]
then
  echo "No dir given"
  exit 1
fi

DIR="$1"

./fc_lc.sh "$1"

pushd "$1"
for cm in EpochSS  FullSS  ViewSS
do
  pushd $cm
  
  mkdir fc_bad
  mkdir lc_bad
  
  for lc_fc in lc fc
  do
    pushd $lc_fc
  
    for test in $(listSubdirs)
    do
      cnt=$(grep UpSnap $test/1.1 | wc -l)
      if (( $cnt != 1 ))
      then
        if [[ "$cm" == FullSS && "$DIR" == "3_all.out" ]]
        then
          : # there are no snapshots here, right?
        else
          mv $test ../${lc_fc}_bad
        fi
      fi
    done
  
    doStuffInMainDir $(listSubdirs)
    
    for test in $(listSubdirs)
    do
      if [[ -e ${test}/badTcp || -e ${test}/badNoRec ]]
      then
        mv $test ../${lc_fc}_bad
      fi
    done
    
    popd
  done
  
  popd
done
popd


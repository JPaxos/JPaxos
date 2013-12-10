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
  
  if [[ "$cm" == FullSS && ( "$DIR" == "3_all.out" || "$DIR" == "3D_all.out" ) ]]
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
  analyzeCrashed=
  analyzeFollower=
  analyzeLeader=
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
    
    analyzeCrashed="${analyzeCrashed} ${d}/1_rps"
    
    lastView=$(grep -h 'View prepared' {0,2}.0_shifted | sort -n | tail -n1 | cut -f 4 -d ' ')
    
    if (( ( lastView % 3 ) == 0 ))
    then
      analyzeFollower="${analyzeFollower} ${d}/2_rps"
      analyzeLeader="${analyzeLeader} ${d}/0_rps"
    else
      analyzeFollower="${analyzeFollower} ${d}/0_rps"
      analyzeLeader="${analyzeLeader} ${d}/2_rps"
    fi
    
    echo -ne "$d," >> ../recStats
    cat recStats >> ../recStats
    popd
  done

  echo "$analyzeLeader" > l_list
  echo "$analyzeFollower" > f_list
  
  java -cp "$base"/averageFromRanges.jar averageFromRanges $analyzeLeader > l_avg
  java -cp "$base"/averageFromRanges.jar averageFromRanges $analyzeCrashed > c_avg
  java -cp "$base"/averageFromRanges.jar averageFromRanges $analyzeFollower > f_avg
  
  "$base"/exclude_outliers.sh
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
  if [[ ! -d "$cm" ]]
  then
    echo "Missing $cm"
    continue
  fi

  
  pushd $cm
  
  mkdir -p fc_bad fc_nan
  mkdir -p lc_bad lc_nan
  
  for lc_fc in lc fc
  do
  
    if [[ ! -d "$lc_fc" ]]
    then
      echo "Missing $lc_fc"
      continue
    fi

    
    pushd $lc_fc
  
    for test in $(listSubdirs)
    do
      cnt=$(grep UpSnap $test/1.1 | wc -l)
      if (( $cnt != 1 ))
      then
        if [[ "$cm" == FullSS && ( "$DIR" == "3_all.out" || "$DIR" == "3D_all.out" ) ]]
        then
          : # there are no snapshots here, right?
        else
          mv $test ../${lc_fc}_bad
          continue
        fi
      fi
      
      rps0=$(grep RPS $test/0.0 | wc -l)
      rps2=$(grep RPS $test/2.0 | wc -l)
      if (( rps0 - rps2 >= 5 || rps0 - rps2 <= -5 ))
      then
        mv $test ../${lc_fc}_nan
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


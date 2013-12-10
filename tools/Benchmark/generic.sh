NORM='\033[00m'
BOLD='\033[00m\033[01m'
REDB='\033[00m\033[01m\033[31m'
HALF='\033[00m\033[02m'

if (( $# != 1 ))
then
    echo "Give test file as argument"
    exit 1
fi

if [[ ! -r "$1" ]]
then
    echo "Test file incorrect/unaccessible"
    exit 2
fi

trap "killall -9 java; exit 1;" SIGINT

cd ../

{

rm -f client__* replica__*

echo -e "${REDB}Starting the benchmark file ${1}${NORM}"

time java -cp Benchmark/bin/ benchmark.Benchmark Benchmark/$1

#echo -e "${REDB}Packing logs...${NORM}"
#rm Benchmark/$1.tar*
#tar czf Benchmark/$1.tar.gz client__* replica__*

echo -e "${REDB}Finished${NORM}"

} 2>&1 | tee genericBenchmark.log


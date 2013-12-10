NORM='\033[00m'
BOLD='\033[00m\033[01m'
REDB='\033[00m\033[01m\033[31m'
HALF='\033[00m\033[02m'

cd ../

echo -e "${REDB}Starting the benchmark example.txt${NORM}"

time java -cp Benchmark/bin/ benchmark.Benchmark Benchmark/example.txt

tar cjf Benchmark/example__logs.tbz2 client__* replica__*

rm -f client__* replica__*

echo -e "${REDB}Finished${NORM}"

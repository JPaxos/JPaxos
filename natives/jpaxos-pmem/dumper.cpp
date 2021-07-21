#include "jpaxos-common.hpp"

int main(int argc, char ** argv){
    if(argc!=2){
        printf("Usage: %s <path_to_pmem_file>\n", argv[0]);
        return 1;
    }
    dumpJpaxosPmem(argv[1], stdout);
    return 0;
}

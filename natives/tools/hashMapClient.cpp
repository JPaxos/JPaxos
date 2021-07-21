#include "jpaxosMClient.h"

#include <stdexcept>
#include <random>
#include <regex>
#include <string>
#include <getopt.h>

using namespace std::literals;

enum reqType : char {
    GET = 'G',
    PUT = 'P'
};

std::uniform_int_distribution<uint32_t> keyDist;

int main(int argc, char ** argv) {
    option opts[]{
        {"reqsize",   required_argument, nullptr, 's'},
        {"keyspace",  required_argument, nullptr, 'k'},
        {"randomize", no_argument,       nullptr, 'R'},
        {"reconnect", optional_argument, nullptr, 'r'},
        {}
    };
    int opt;
    uint8_t required = 0;
    while(-1 != (opt = getopt_long(argc, argv, "s:k:Rr::", opts, nullptr))){
        switch(opt){
            case 's':
                reqSize = std::stoul(optarg);
                required |= 0x01;
                break;
            case 'k':
                keyDist = std::uniform_int_distribution<uint32_t>(0, std::stoul(optarg)-1);
                required |= 0x02;
                break;
            case 'R':
                randomizeEachRequest = true;
                break;
            case 'r':
                reconnectPeriodically = true;
                if(optarg)
                    reconnectPeriodicallyEvery = std::stoul(optarg);
                break;
            default:
                throw std::invalid_argument("unknown option");
        }
    }
    if(required != 0x03)
        throw std::invalid_argument("some mandatory option missing");
    
    return jpaxosMClientMain();
}

inline void randomize(std::vector<char> & request) {
    uint8_t step = sizeof(gen.max());
    char * cursor = request.data() + requestHeaderSize + 9;
    for (int i = request.size() - step - requestHeaderSize + 9 ; i > 0 ; i -= step) {
        cursor+=step;
        *( (decltype(gen.max())* ) cursor) = gen();
    }
    cursor = request.data() + request.size() - step;
    *( (decltype(gen.max())* ) cursor) = gen();
}

void generateRequest(const uint64_t, const uint32_t, std::vector<char> & request){
    if(gen()%2){
        request.resize(requestHeaderSize+9);
        *( uint8_t*)(request.data()+requestHeaderSize+0) = GET;                   // req type
        *(uint32_t*)(request.data()+requestHeaderSize+1) = fromBE(4);             // key size
        *(uint32_t*)(request.data()+requestHeaderSize+5) = fromBE(keyDist(gen));  // key
    } else {
        request.resize(requestHeaderSize+reqSize);
        *( uint8_t*)(request.data()+requestHeaderSize+0) = PUT;                   // req type
        *(uint32_t*)(request.data()+requestHeaderSize+1) = fromBE(4);             // key size
        *(uint32_t*)(request.data()+requestHeaderSize+5) = fromBE(keyDist(gen));  // key
        if(randomizeEachRequest)                                                  // remining bytes are the value
            randomize(request);
    }
}

void parseResponse(const uint64_t, const uint32_t, const std::vector<char> &, const std::vector<char> &){
    // ignoring response
}

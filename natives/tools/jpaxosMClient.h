#ifndef JPAXOSMCLIENT_H_INCLUDED
#define JPAXOSMCLIENT_H_INCLUDED

#include <cstdint>
#include <vector>
#include <random>
#include <bit>

extern std::default_random_engine gen;

constexpr size_t requestHeaderSize  = 20;

// if main() is overwritten, then reqSize shall be assigned in the new main()
extern uint32_t reqSize;
extern bool randomizeEachRequest;

// if set, every xxx requests each client changes the replica
extern bool reconnectPeriodically;
extern uint32_t reconnectPeriodicallyEvery;

/* • req is initialized to size "reqSize + requestHeaderSize"
   • resizing it is ok, but beware:
   • req must reserve requestHeaderSize bytes on the beginning for the JPaxos request header  */
void generateRequest(const uint64_t clientId, const uint32_t cliSeqNo, std::vector<char> & req);

void parseResponse(const uint64_t clientId, const uint32_t cliSeqNo, const std::vector<char> & request, const std::vector<char> & response);

// if main() is overwritten, this shall be invoked to read paxos.properties and execure the main loop
int jpaxosMClientMain();


/* ~ ~ ~ ~ ~ ~ generic helper functions ~ ~ ~ ~ ~ ~ */

constexpr inline uint32_t fromBE(const uint32_t in) {
    if constexpr (std::endian::native == std::endian::little) {
        return ((in >> 24) & 0xFF) <<  0 | 
               ((in >> 16) & 0xFF) <<  8 |
               ((in >>  8) & 0xFF) << 16 |
               ((in >>  0) & 0xFF) << 24 ;
    } else {
        return in;
    }
}

template <typename I = uint64_t> requires (std::is_unsigned<I>::value)
inline void randomize(char * begin, char * end) {
    static std::uniform_int_distribution<I> distWide;
    static std::uniform_int_distribution<uint8_t> distSingle;
    char * cursor = begin;
    for (;cursor <= end - sizeof(I); cursor += sizeof(I))
        *(I*)cursor = distWide(gen);
    for (;cursor < end; cursor++)
        *(uint8_t*) cursor = distSingle(gen);
}

#endif // JPAXOSMCLIENT_H_INCLUDED

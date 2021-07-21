#include <cstdint>
#include <vector>
#include <queue>
#include <string>
#include <chrono>
#include <regex>
#include <iostream>
#include <fstream>
#include <random>
#include <cassert>
#include <cstdarg>

#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <sys/signal.h>

#include "jpaxosMClient.h"
#include "backtrace_error.h"

using namespace std::literals;

enum LogLevel : uint32_t {TRACE=0, DEBUG=10, INFO=20, ERROR=30};
inline void log(LogLevel lvl, const char* fmt...);

#ifndef MAX_LOG_LEVEL
#define MAX_LOG_LEVEL TRACE
#endif

#ifdef NOLOG
void log(LogLevel, const char* ...) {}
#else
void log(LogLevel lvl, const char* fmt...) {
    if(lvl < MAX_LOG_LEVEL)
        return;
    
    std::chrono::duration<double> ts = std::chrono::system_clock::now().time_since_epoch();
    
    switch(lvl) {
        case ERROR:
            fprintf(stdout, "%.6lf [ERROR]", ts.count());
            break;
        case INFO:
            fprintf(stdout, "%.6lf [ INFO]", ts.count());
            break;
        case DEBUG:
            fprintf(stdout, "%.6lf [DEBUG]", ts.count());
            break;
        case TRACE:
            fprintf(stdout, "%.6lf [TRACE]", ts.count());
            break;
        default:
            fprintf(stdout, "%.6lf [%5u]" , ts.count(), lvl);
    };
    va_list args;
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    fprintf(stdout, "\n");
    fflush(stdout);
}
#endif

  // Connect: 
  // send 'T', receive uint64_t clientId
  // send 'F', send    uint64_t clientId

  // Request:  uint32 command type    (==0)  +  uint64 clientId  +  uint32 seqNo  +  uint32 length  +  uint8×length request
  
  // Reply:    uint32 reply OK        (==0)  +  uint32 length        +  uint8×length request
  // Reply:    uint32 reply REDIRECT  (==2)  +  uint32 length (==4)  +  uint32 new primary ID
  // Reply:    uint32 reply RECONNECT (==3)  +  uint32 length (==0)  

enum ResultType : uint32_t {
    OK        = fromBE(0),
    NACK      = fromBE(1),
    REDIRECT  = fromBE(2),
    RECONNECT = fromBE(3) 
};

constexpr std::chrono::milliseconds connectionTimeout =  500ms;
constexpr std::chrono::milliseconds connRetryTimeout  =   50ms;
constexpr std::chrono::milliseconds idTimeout         =  500ms;
constexpr std::chrono::milliseconds minRequestTimeo   =  100ms;
constexpr std::chrono::milliseconds initRequestRtt    = 1000ms;

constexpr int rttToTimeoutMult = 3;
constexpr double rttConvFactor = 0.2;

constexpr size_t responseHeaderSize = 8;

std::random_device rd;
std::default_random_engine gen(rd());
  
struct EpollEventHandler {
    virtual void handleEpollEvent(uint32_t events) = 0;
    virtual ~EpollEventHandler(){};
};

struct Client : public EpollEventHandler {
    Client();
    Client(const Client & other) = delete;
    
    enum {WAITING_FOR_CONNECT, WAITING_FOR_RECONNECT, WAITING_FOR_ID, WAITING_FOR_RESPONSE_HEADER, WAITING_FOR_RESPONSE_BODY, IDLE, SENDING_REQUEST} state;
    
    uint64_t clientId = UINT64_MAX;
    uint32_t cliSeqNo = UINT32_MAX;
    
    uint64_t remainingRequests = 0;
    
    std::vector<int> reconnectOrder;
    
    int socket = -1;
    
    size_t bytesTransferred = 0;
    
    uint32_t reconnectPeriodicallyRemaining;
    
    std::vector<char> request;
    bool requestAlreadyGenerated = false;
    
    size_t responseSize;
    char headerBuffer[responseHeaderSize];
    std::vector<char> responseBuffer;
    
    void terminate();
    void reconnect();
    
    void initSending(uint64_t count);
    void checkIfThereIsWorkToDo();
    void sendRequest();
    virtual void handleEpollEvent(uint32_t events) override;
    void timedOut();
    
    std::chrono::duration<double> requestRtt = initRequestRtt;
    std::chrono::system_clock::time_point requestSendTs = std::chrono::system_clock::time_point::min();
    void updateAgvRespTime();
    
    std::chrono::system_clock::time_point nextTimeout;
    template <typename T>
    void updateTimeout(T timeout);
    void disableTimeout();
};

inline void randomize(std::vector<char> & request) {
    uint8_t step = sizeof(gen.max());
    char * cursor = request.data() + requestHeaderSize;
    for (int i = request.size() - step - requestHeaderSize; i > 0 ; i -= step) {
        cursor+=step;
        *( (decltype(gen.max())* ) cursor) = gen();
    }
    cursor = request.data() + request.size() - step;
    *( (decltype(gen.max())* ) cursor) = gen();
}

void __attribute__((weak)) generateRequest(const uint64_t, const uint32_t, std::vector<char> & request) {
    if(randomizeEachRequest)
        randomize(request);
}

void __attribute__((weak)) parseResponse(const uint64_t, const uint32_t, const std::vector<char> & , const std::vector<char> & ){
    // empty on purpose
}

int epollfd;

uint32_t reqSize = 1024;
bool randomizeEachRequest = false;
bool reconnectPeriodically = false;
uint32_t reconnectPeriodicallyEvery = 1000;

std::map<int, addrinfo> replicas;
std::vector<Client*> clients;
uint64_t stillRunningClients;

std::map<std::chrono::system_clock::time_point, std::vector<Client*>> timeouts;

std::queue <std::string> pendingCommands;

Client::Client() {
    log(DEBUG, "%p client created", this);
    if(reconnectPeriodically)
        reconnectPeriodicallyRemaining = std::uniform_int_distribution<>(1,reconnectPeriodicallyEvery)(gen);
    reconnect();
}

void Client::reconnect() {
    terminate();
    
    if(reconnectOrder.empty()) {
        for(auto & [k, v] : replicas)
            reconnectOrder.push_back(k);
        std::shuffle(reconnectOrder.begin(), reconnectOrder.end(), gen);
    }
    auto & ai = replicas[reconnectOrder.back()];
    log(DEBUG, "%08lx (%p) reconnecting to %u", clientId, this, reconnectOrder.back());
    reconnectOrder.pop_back();
    
    socket = ::socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol);
    if(socket == -1) {
        if(errno==ENFILE)
            throw backtrace_error("too many files open - limit hit?");
        throw backtrace_error("socket");
    }
    if(-1 == fcntl(socket, F_SETFL, fcntl(socket, F_GETFL) | O_NONBLOCK)) {
        throw backtrace_error("fcntl");
    }
    state = WAITING_FOR_CONNECT;
    updateTimeout(connectionTimeout);
    
    epoll_event ee{.events=EPOLLIN|EPOLLOUT|EPOLLRDHUP, .data={.ptr=this}};
    if(-1 == epoll_ctl(epollfd, EPOLL_CTL_ADD, socket, &ee))
        throw backtrace_error("epoll_ctl add");
    
    int res = connect(socket, ai.ai_addr, ai.ai_addrlen);
    
    if(res == -1 && errno == EINPROGRESS)
        return;
    
    if(res == -1)
        log(INFO, "%08lx (%p) async connect failed - %s", clientId, this, strerror(errno));
    
    handleEpollEvent(EPOLLOUT);
}

void Client::terminate() {
    if(socket==-1)
        return;
    log(TRACE, "%08lx (%p) closing old socket", clientId, this, reconnectOrder.back());
    shutdown(socket, SHUT_RDWR);
    close(socket);
    socket=-1;
}

void Client::initSending(uint64_t count) {
    request.resize(requestHeaderSize+reqSize);
    remainingRequests = count;
    checkIfThereIsWorkToDo();
}

void Client::checkIfThereIsWorkToDo() {
     if(state == WAITING_FOR_CONNECT ||state == WAITING_FOR_RECONNECT || state == WAITING_FOR_ID)
        return;
     if(remainingRequests == 0) {
        log(INFO, "%08lx idling", clientId);
        stillRunningClients--;
        state = IDLE;
        disableTimeout();
        return;
    }
    if(reconnectPeriodically){
        if(!reconnectPeriodicallyRemaining--){
            log(INFO, "%08lx periodical reconnect", clientId);
            reconnectPeriodicallyRemaining = reconnectPeriodicallyEvery;
            reconnect();
            return;
        }
    }
    sendRequest();
}


void Client::sendRequest() {
    if(!requestAlreadyGenerated){
        requestAlreadyGenerated=true;
        generateRequest(clientId, cliSeqNo, request);
        *(uint32_t*)(request.data()+ 0) = 0;
        *(uint64_t*)(request.data()+ 4) = clientId;
        *(uint32_t*)(request.data()+12) = fromBE(cliSeqNo);
        *(uint32_t*)(request.data()+16) = fromBE(request.size()-requestHeaderSize);
    }
    
    auto timeout = requestRtt*rttToTimeoutMult;
    if(timeout < minRequestTimeo)
        timeout = minRequestTimeo;
    updateTimeout(std::chrono::duration_cast<std::chrono::microseconds>(timeout));
    
    if(requestSendTs == std::chrono::system_clock::time_point::min())
        requestSendTs = std::chrono::system_clock::now();
    
    int written = write(socket, request.data(), request.size());
  
    log(TRACE, "%08lx sending request (wrote %d)", clientId, written);
    
    if(written > 0 && ((unsigned)written == request.size())) {
        bytesTransferred = 0;
        state = WAITING_FOR_RESPONSE_HEADER;
        return;
    }
    
    if(written == -1) {
        if(errno != EAGAIN && errno != EWOULDBLOCK) {
            log(INFO, "%08lx sending request failed (%s)", clientId, strerror(errno));
            reconnect();
            return;
        }
        bytesTransferred = 0;
    } else
        bytesTransferred = written;
    
    state = SENDING_REQUEST;
            
    epoll_event ee{.events=EPOLLIN|EPOLLOUT|EPOLLRDHUP, .data{.ptr=this}};
    if(-1 == epoll_ctl(epollfd, EPOLL_CTL_MOD, socket, &ee))
        throw backtrace_error("epoll_ctl SENDING_REQUEST");
}

void Client::handleEpollEvent(uint32_t events) {
    if(state != WAITING_FOR_CONNECT && events & ~(EPOLLIN|EPOLLOUT)) {
        log(INFO, "%08lx (%p) epoll reported an error 0x%x on socket", clientId, this, events);
        reconnect();
    }
    
    if(state == WAITING_FOR_RESPONSE_HEADER) {
        int res = read(socket, headerBuffer + bytesTransferred, responseHeaderSize - bytesTransferred);
        log(TRACE, "%08lx WAITING_FOR_RESPONSE_HEADER: read %d of %d", clientId, res, responseHeaderSize - bytesTransferred);
        if(res==-1) {
            log(INFO, "%08lx reading response failed (%s)", clientId, strerror(errno));
            reconnect();
            return;
        }
        bytesTransferred += res;
        if(bytesTransferred == responseHeaderSize) {
            const ResultType & type = *((ResultType*)(headerBuffer+0));
            const uint32_t   & size = *((uint32_t*  )(headerBuffer+4));
            log(TRACE, "%08lx response header: type %d, size %d", clientId, fromBE(type), fromBE(size));
            if(type == RECONNECT) {
                log(INFO, "%08lx got a RECONNECT response", clientId);
                reconnect();
                return;
            }
            if(type == REDIRECT) {
                uint32_t tgt;
                res = read(socket, &tgt, 4);
                if(res!=4)
                    log(ERROR, "%08lx got a REDIRECT response and the programmer is too lazy to fully support it", clientId);
                else {
                    tgt = fromBE(tgt);
                    reconnectOrder.push_back(tgt);
                    log(INFO, "%08lx got a REDIRECT response to %u", clientId, tgt);
                }
                reconnect();
                return;
            }
            if(type != OK){
                if(type == NACK)
                    throw backtrace_error("Got a NACK response");
                throw backtrace_error(("unsupported response type " + std::to_string(fromBE(type))).c_str());
            }
            
            state = WAITING_FOR_RESPONSE_BODY;
            responseSize = fromBE(size);
            responseBuffer.resize(responseSize);
            bytesTransferred = 0;
        }
    }
    if(state == WAITING_FOR_RESPONSE_BODY) {
        int res = read(socket, responseBuffer.data() + bytesTransferred, responseSize - bytesTransferred);
        log(TRACE, "%08lx WAITING_FOR_RESPONSE_BODY: read %d of %d", clientId, res, responseSize - bytesTransferred);
        if(res == -1) {
            if(errno != EAGAIN && errno != EWOULDBLOCK) {
                log(INFO, "%08lx reading response failed (%s)", clientId, strerror(errno));
                reconnect();
            }
            return;
        }
        bytesTransferred += res;
        if(bytesTransferred == responseSize) {
            log(DEBUG, "%08lx got response for %u", clientId, cliSeqNo);
            requestAlreadyGenerated = false;
            updateAgvRespTime();
            parseResponse(clientId, cliSeqNo, request, responseBuffer);
            remainingRequests--;
            cliSeqNo++;
            checkIfThereIsWorkToDo();
        }
        return;
    }
    if(state == WAITING_FOR_CONNECT) {
        int32_t last_error;
        socklen_t optsize = 4;
        if(0 != getsockopt(socket, SOL_SOCKET, SO_ERROR, &last_error, &optsize))
            throw backtrace_error("getsockopt");
        if(last_error != 0) {
            log(DEBUG, "%08lx (%p) connect failed (%s)", clientId, this, strerror(last_error));
            terminate();
            state = WAITING_FOR_RECONNECT;
            updateTimeout(connRetryTimeout);            
            return;
        }
        
        log(DEBUG, "%08lx (%p) connect successful", clientId, this);
        
        epoll_event ee{.events=EPOLLIN|EPOLLRDHUP, .data={.ptr=this}};
        if(-1 == epoll_ctl(epollfd, EPOLL_CTL_MOD, socket, &ee))
            throw backtrace_error("epoll_ctl WAITING_FOR_CONNECT");
        
        if(cliSeqNo == UINT32_MAX && clientId == UINT64_MAX) {
            char msg = 'T';
            int res = write(socket, &msg, 1);
            if(res!=1) {
                if(errno == EAGAIN || errno == EWOULDBLOCK) {
                    log(DEBUG, "%08lx (%p) write on newly connected socket would block", clientId, this);
                    return;
                }
                log(ERROR, "%08lx (%p) write on newly connected socket failed (%s)", clientId, this, strerror(errno));
                reconnect();
                return;
            }
            updateTimeout(idTimeout);
            state = WAITING_FOR_ID;
        } else {
            char msg[9];
            msg[0]='F';
            *((uint64_t*)(msg+1)) = clientId;
            int res = write(socket, &msg, 9);
            if(res!=9) {
                log(ERROR, "%08lx write on newly connected socket failed (%s)", clientId, strerror(errno));
                reconnect();
                return;
            }
            state = IDLE;
            checkIfThereIsWorkToDo();
        }
        return;
    }
    if(state == WAITING_FOR_ID) {
        int res = read(socket, &clientId, 8);
        if(res != 8) {
            log(ERROR, "%08lx (%p) did not get 8 bytes for ID (%s)", clientId, this, strerror(res));
            clientId = UINT64_MAX;
            reconnect();
            return;
        }
        cliSeqNo = 0;
        log(DEBUG, "%08lx (%p) got ID", clientId, this);
        state = IDLE;
        checkIfThereIsWorkToDo();
        return;
    }
    if(state == SENDING_REQUEST) {
        int written = write(socket, request.data() + bytesTransferred, request.size() - bytesTransferred);
    
        log(TRACE, "%08lx SENDING_REQUEST: wrote %d", clientId, written);
        
        if(written == -1) {
            log(INFO, "%08lx sending request failed (%s)", clientId, strerror(errno));
            reconnect();
            return;
        }
        bytesTransferred += written;
        if (bytesTransferred == request.size()) {
            epoll_event ee;
            ee.data.ptr = this;
            ee.events = EPOLLIN|EPOLLRDHUP;
            epoll_ctl(epollfd, EPOLL_CTL_MOD, socket, &ee);
            bytesTransferred = 0;
            state = WAITING_FOR_RESPONSE_HEADER;
        }
        return;
    }
    throw backtrace_error("Unhandled state " + std::to_string(state));
}

void Client::timedOut() {
    if(state==WAITING_FOR_RECONNECT)
        log(DEBUG, "%08lx (%p) reconnect grace period expired", clientId, this);
    else
        log(INFO, "%08lx (%p) timed out", clientId, this);
    reconnect();
}

void Client::disableTimeout() {
    log(TRACE, "%08lx (%p) disabling timeout", clientId, this);
    auto it = timeouts.find(nextTimeout);
    if(it != timeouts.end()) {
        std::erase(it->second, this);
        if(it->second.empty())
            timeouts.erase(it);
    }
}

template <typename T>
void Client::updateTimeout(T timeout) {
    disableTimeout();
    nextTimeout = std::chrono::system_clock::now() + timeout;
    log(TRACE, "%08lx (%p) timeout is %.3lf ms, waking at %.6lf",
        clientId, this,
        std::chrono::duration<double, std::milli>(timeout).count(),
        std::chrono::duration<double>(nextTimeout.time_since_epoch()).count()
    );
    timeouts[nextTimeout].push_back(this);
}

void Client::updateAgvRespTime() {
    auto rtt = std::chrono::system_clock::now() - requestSendTs;
    requestSendTs = std::chrono::system_clock::time_point::min();
    requestRtt = requestRtt*(1-rttConvFactor) + rtt* rttConvFactor;
    log(TRACE, "%08lx request took %.3lf ms, new avg rtt is %.3lf ms", clientId,
        std::chrono::duration<double, std::milli>(rtt).count(),
        std::chrono::duration<double, std::milli>(requestRtt).count()
    );
}

void terminateAll() {
    for(Client * c : clients)
        c->terminate();
}

void doRequests(uint64_t numberOfClients, uint64_t requestPerClient) {
    stillRunningClients = numberOfClients;
    
    if(clients.size() < numberOfClients) {
        auto it = clients.size(); 
        clients.resize(numberOfClients);
        
        for(; it < numberOfClients; ++it)
            clients[it] = new Client;
    }
        
    for(uint64_t i = 0; i < numberOfClients ; ++i)
        clients[i]->initSending(requestPerClient);
    
    // reads 'kill' command from stdin
    struct : public EpollEventHandler {
        std::string line;
        inline void dieIf(int res, std::string txt) {
            if(res==-1) throw backtrace_error("STDIN " + txt);
        }
        void handleEpollEvent(uint32_t events) override {
            if(events != EPOLLIN) dieIf(-1, "error1");
            
            auto oldflags = fcntl(STDIN_FILENO, F_GETFL);
            dieIf(oldflags, "getfl");
            dieIf(fcntl(STDIN_FILENO, F_SETFL, oldflags|O_NONBLOCK), "setfl1");
            
            char c;
            while(1) {
                int count = read(STDIN_FILENO, &c, 1);
                if(count==-1) {
                    if(errno != EAGAIN && errno != EWOULDBLOCK) dieIf(-1, "error");
                    dieIf(fcntl(STDIN_FILENO, F_SETFL, oldflags), "setfl2");
                    return;
                } else dieIf(count-1, "EOF");
                if(c == '\r') continue;
                if(c == '\n') break;
                line += c;
            }
            
            if(line == "kill") {
                log(INFO, "Got a kill command");
                raise(SIGINT);
            } else {
                log(DEBUG, "Got  \"%s\" while still doing previous command", line.c_str());
                pendingCommands.push(line);
            }
            
            dieIf(fcntl(STDIN_FILENO, F_SETFL, oldflags), "setfl3");
            line.clear();
        }
    } stdinHandler;
    
    epoll_event ee{.events=EPOLLIN|EPOLLRDHUP, .data={.ptr=&stdinHandler}};
    if(-1 == epoll_ctl(epollfd, EPOLL_CTL_ADD, STDIN_FILENO, &ee))
        throw backtrace_error("epoll_ctl add STDIN_FILENO");
     
    while(stillRunningClients > 0) {
        assert(!timeouts.empty());
        auto now = std::chrono::system_clock::now();
        while(timeouts.begin()->first <= now) {
            auto node = timeouts.extract(timeouts.begin());
            for(Client* c : node.mapped())
                c->timedOut();
            now = std::chrono::system_clock::now();
        }
        
        auto toWait = std::chrono::duration_cast<std::chrono::milliseconds>( timeouts.begin()->first - now);
        
        epoll_event ee[16];
        int res = epoll_wait(epollfd, ee, 16, toWait.count());
        if(res == -1)
            throw backtrace_error("epoll_wait");
        for(int i = 0 ; i < res; ++i)
            ((Client*)ee[i].data.ptr)->handleEpollEvent(ee[i].events);
    }
    
    if(-1 == epoll_ctl(epollfd, EPOLL_CTL_DEL, STDIN_FILENO, nullptr))
        throw backtrace_error("epoll_ctl del STDIN_FILENO");
}

void handleSignals(int) {
    terminateAll();
    for(auto c : clients)
        delete c;
    clients.clear();
    close(epollfd);
    exit(1);
}

int __attribute__((weak)) main(int argc, char ** argv) {
    if(argc>1)
        reqSize = std::stol(argv[1]);
    
    if(argc>2) {
        if(std::regex_match(argv[2], std::regex("(true)|(yes)", std::regex::icase|std::regex::ECMAScript)))
            randomizeEachRequest = true;
        else if (!std::regex_match(argv[2], std::regex("(false)|(no)", std::regex::icase|std::regex::ECMAScript)))
            throw std::invalid_argument(argv[2] + " is not true/yes/false/no"s);
    }
    
    if(argc>3) {
        reconnectPeriodically = true;
        reconnectPeriodicallyEvery = std::stol(argv[3]);
        if(reconnectPeriodicallyEvery<=0)
            throw std::invalid_argument(argv[3] + " is not positive"s);
    }
    
    return jpaxosMClientMain();
}

int jpaxosMClientMain(){
    std::ifstream paxosproperties("paxos.properties");
    if(!paxosproperties) throw backtrace_error("paxosproperties");
    
    while(paxosproperties) {
        std::string line;
        std::getline(paxosproperties, line);
        std::smatch m;
        if(std::regex_match(line, m, std::regex("process.(\\d+)\\s*=\\s*([^:]+):\\d+:(\\d+)", std::regex::icase|std::regex::ECMAScript))) {
            addrinfo hints{}, *out;
            hints.ai_socktype = SOCK_STREAM;
            auto res = getaddrinfo(m[2].str().c_str(), m[3].str().c_str(), &hints, &out);
            if(res == -1 || out == nullptr)
                throw backtrace_error("getaddrinfo");
            replicas[std::stoi(m[1])] = *out;
            // DO NOT freeaddrinfo(out);
        }
    }
    if(replicas.empty()) throw backtrace_error("no replicas");
    
    std::string replicasString = "Replicas:";
    for(auto & [k,v] : replicas) {
        char host[NI_MAXHOST], port[NI_MAXSERV];
        getnameinfo(v.ai_addr, v.ai_addrlen, host, NI_MAXHOST, port, NI_MAXSERV, NI_NUMERICHOST|NI_NUMERICSERV);
        replicasString += "\n"s + "  [" + std::to_string(k) + "] " + host + ":" + port;
    }
    log(DEBUG, "request size: %u", reqSize);
    log(DEBUG, "randomize: %s", randomizeEachRequest ? "true" : "false");
    log(DEBUG, "%s", replicasString.c_str());
    
    
    epollfd = epoll_create1(0);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, handleSignals);
    signal(SIGINT, handleSignals);
    signal(SIGTERM, handleSignals);

    if(epollfd==-1) throw backtrace_error("epoll_create1");
    
    while(true) {
        std::cout << "bye / kill - exits" << std::endl << "<clientCount> <requestsPerClient> [<ignored option>] - inits sending" << std::endl;
        std::string line;
        if(pendingCommands.empty())
            std::getline(std::cin, line);
        else {
            line = pendingCommands.front();
            pendingCommands.pop();
        }
        
        log(DEBUG, "processing command: %s", line.c_str());
        
        if(std::regex_match(line, std::regex("(bye)|(kill)", std::regex::icase|std::regex::ECMAScript))) {
            terminateAll();
            break;
        }
        
        std::smatch matched;
        if(std::regex_match(line, matched, std::regex("(\\d+)\\s+(\\d+)(\\s+\\S+)?", std::regex::icase|std::regex::ECMAScript))) {
            uint64_t clients = std::stoll(matched[1]);
            uint64_t requestPerClient = std::stoll(matched[2]);
            doRequests(clients, requestPerClient);
            // line below is must match "^Finished.*" so Benchmark.jar learns that we're done
            std::cout << "Finished" << std::endl;
            continue;
        }
        
        std::cout << "not understood" << std::endl;
    }
    return 0;
}

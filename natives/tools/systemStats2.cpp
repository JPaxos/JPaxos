#include <regex>
#include <vector>
#include <cstdio>
#include <unistd.h>
#include <errno.h>
#include <error.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>   
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>

/*
# iptables-save 
# Generated by iptables-save v1.6.1 on Wed Nov  6 21:43:34 2019
*filter
:INPUT ACCEPT [106339:43706686]
:FORWARD ACCEPT [0:0]
:OUTPUT ACCEPT [103336:9268580]
-A INPUT -p tcp -m tcp --sport 3000:3999
-A INPUT -p tcp -m tcp --dport 3000:3999
-A INPUT -p tcp -m tcp --dport 2000:2999
-A OUTPUT -p tcp -m tcp --sport 3000:3999 -j MARK --set-xmark 0x3/0xffffffff
-A OUTPUT -p tcp -m tcp --dport 3000:3999 -j MARK --set-xmark 0x3/0xffffffff
-A OUTPUT -p tcp -m tcp --sport 2000:2999 -j MARK --set-xmark 0x2/0xffffffff
COMMIT
# Completed on Wed Nov  6 21:43:34 2019
*/

/*
iptables -xvnL
Chain INPUT (policy ACCEPT 7 packets, 13399 bytes)
    pkts      bytes target     prot opt in     out     source               destination         
       0        0            tcp  --  *      *       0.0.0.0/0            0.0.0.0/0            tcp spts:3000:3999
       0        0            tcp  --  *      *       0.0.0.0/0            0.0.0.0/0            tcp dpts:3000:3999
       0        0            tcp  --  *      *       0.0.0.0/0            0.0.0.0/0            tcp dpts:2000:2999

Chain FORWARD (policy ACCEPT 0 packets, 0 bytes)
    pkts      bytes target     prot opt in     out     source               destination         

Chain OUTPUT (policy ACCEPT 7 packets, 364 bytes)
    pkts      bytes target     prot opt in     out     source               destination         
       0        0 MARK       tcp  --  *      *       0.0.0.0/0            0.0.0.0/0            tcp spts:3000:3999 MARK set 0x3
       0        0 MARK       tcp  --  *      *       0.0.0.0/0            0.0.0.0/0            tcp dpts:3000:3999 MARK set 0x3
       0        0 MARK       tcp  --  *      *       0.0.0.0/0            0.0.0.0/0            tcp spts:2000:2999 MARK set 0x2
*/

unsigned long long totalInput    , totalInput_prev    ;
unsigned long long totalOutput   , totalOutput_prev   ;

unsigned long long replicaInput  , replicaInput_prev  ;
unsigned long long replicaOutput , replicaOutput_prev ;

unsigned long long clientInput   , clientInput_prev   ;
unsigned long long clientOutput  , clientOutput_prev  ;

timeval previous{};

std::string runIptables(){
    constexpr size_t maxOutputSize = 2048;
    std::string output;
    output.resize(maxOutputSize);
    
    int iptablesPipe[2];
    if(pipe(iptablesPipe))
        error(1, errno, "plumbing problems");
    
    if(pid_t pid = fork(); pid){
        if(pid==-1)
            error(1, errno, "there is no spoon");
        close(iptablesPipe[1]);
        
        for(int totalBytes = 0;;){
            int bytesRead = read(iptablesPipe[0], output.data(), maxOutputSize-totalBytes);
            if(bytesRead==0){
                close(iptablesPipe[0]);
                output.resize(totalBytes);
                break;
            }
            if(bytesRead==-1)
                error(1,errno, "Reading form iptables failed");
            totalBytes+=bytesRead;
            if(totalBytes==maxOutputSize)
                error(1, 0, "Too much data from iptables - got:\n%s", output.c_str());
        }
        int status;
        if(pid!=waitpid(pid, &status, 0))
            error(1, errno, "bad child");
        if(!WIFEXITED(status))
            error(1, errno, "iptables failed");
    } else {
        close(0);
        dup2(iptablesPipe[1],STDOUT_FILENO);
        execl("/usr/sbin/iptables", "iptables", "-xvnL", nullptr);
        error(1, errno, "cannot run iptables");
    }
    
    return output;
}

void getResults(std::string & output){
    
    auto linePattern  = std::regex("[^\n]+\n");
    auto chainPattern = std::regex(R"((\d+) bytes)");
    auto rulePattern  = std::regex(R"(^\s*\d+\s+(\d+)\s)");
    
    std::vector<std::string> lines;
    auto linesIter = std::sregex_iterator(output.begin(), output.end(), linePattern);
    while(linesIter!=std::sregex_iterator()){
        lines.push_back(std::move(linesIter->str()));
        linesIter++;
    }
    
    if(lines.size()!=12)
        error(1,0,"Unexpected output from iptables - it has %ld lines and is:\n%s", lines.size(), output.c_str());
    
    auto getMatch = [&lines, &output](int line, std::regex & pattern){
        std::smatch match;
        if(!std::regex_search(lines[line], match, pattern))
            error(1, 0, "Unexpected output from iptables - line %d does not match the regex\n%s", line, output.c_str());
        return strtoull(match[1].str().data(), nullptr, 10);
    };
    
    totalInput    = getMatch( 0, chainPattern);
    totalOutput   = getMatch( 7, chainPattern);
    
    replicaInput  = getMatch( 2, rulePattern) + getMatch( 3, rulePattern);
    replicaOutput = getMatch( 9, rulePattern) + getMatch(10, rulePattern);
    
    clientInput   = getMatch( 4, rulePattern);
    clientOutput  = getMatch(11, rulePattern);
}

void usr1(int){
    auto output = runIptables();
    
    timeval now;
    gettimeofday(&now, nullptr);
    
    getResults(output);
    
    auto usec = (now.tv_sec - previous.tv_sec)*1000000 + (now.tv_usec - previous.tv_usec);
    previous = now;

    printf("%ld.%06ld", now.tv_sec, now.tv_usec);
    
    #define PRINTERUPDATER(x) \
        printf(" %.2f", (x - x##_prev)*1000000.0/usec); \
        x##_prev = x;

    PRINTERUPDATER(totalInput   );
    PRINTERUPDATER(totalOutput  );

    PRINTERUPDATER(replicaInput );
    PRINTERUPDATER(replicaOutput);

    PRINTERUPDATER(clientInput  );
    PRINTERUPDATER(clientOutput );

    putchar('\n');
    fflush(stdout);
}

int main(int argc, char ** argv) {
    if(geteuid())
        error(1, 0, "This program shall be run as root");
    if(argc != 3){
        printf("Wrong argument count!\n Usage: %s <time_in_ms_from_10_to_999>\n", argv[0]);
        return -1;
    }
    
    int stopPipe =  open(argv[1], O_RDWR);
    if(stopPipe == -1)
        error(1, errno, "Cannot open %s", argv[1]);
    
    long sampling_ms = atoi(argv[2]);
    long sampling_us = sampling_ms*1000l;
    long sampling_ns = sampling_us*1000l;

    signal(SIGUSR1,usr1);
    
    timer_t timer;
    struct sigevent sev{SIGEV_SIGNAL, SIGUSR1, 0, 0};
    timer_create(CLOCK_REALTIME, &sev, &timer);

    struct itimerspec timespec{};
    
    // wake up every sampling rate
    timespec.it_interval.tv_nsec = sampling_ns;
    
    // first wakeup about round time
    timeval tv;
    gettimeofday(&tv, nullptr);
    timespec.it_value.tv_nsec = (sampling_us-tv.tv_usec%sampling_us)*1000l;
    
    timer_settime(timer, 0, &timespec, NULL);

    //while(pause(),1);
    char dummy;
    read(stopPipe, &dummy, 1);
    
    return 0;
}

#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <cstdio>
#include <cstring>
#include  <string>
#include <unordered_map>

FILE* out;

const std::unordered_map<int, const std::string> signames {
    {SIGHUP   ,"SIGHUP   "}, {SIGBUS   ,"SIGBUS   "}, {SIGTSTP  ,"SIGTSTP  "}, {SIGVTALRM,"SIGVTALRM"},
    {SIGINT   ,"SIGINT   "}, {SIGSEGV  ,"SIGSEGV  "}, {SIGCONT  ,"SIGCONT  "}, {SIGPROF  ,"SIGPROF  "},
    {SIGQUIT  ,"SIGQUIT  "}, {SIGSYS   ,"SIGSYS   "}, {SIGCHLD  ,"SIGCHLD  "}, {SIGWINCH ,"SIGWINCH "},
    {SIGILL   ,"SIGILL   "}, {SIGPIPE  ,"SIGPIPE  "}, {SIGTTIN  ,"SIGTTIN  "}, {SIGUSR1  ,"SIGUSR1  "},
    {SIGTRAP  ,"SIGTRAP  "}, {SIGALRM  ,"SIGALRM  "}, {SIGTTOU  ,"SIGTTOU  "}, {SIGUSR2  ,"SIGUSR2  "},
    {SIGABRT  ,"SIGABRT  "}, {SIGTERM  ,"SIGTERM  "}, {SIGPOLL  ,"SIGPOLL  "},
    {SIGFPE   ,"SIGFPE   "}, {SIGURG   ,"SIGURG   "}, {SIGXCPU  ,"SIGXCPU  "},
    {SIGKILL  ,"SIGKILL  "}, {SIGSTOP  ,"SIGSTOP  "}, {SIGXFSZ  ,"SIGXFSZ  "}
};

void action(int, siginfo_t * info, void *){
    fprintf(out, "%s    | signo:%4d  ", signames.contains(info->si_signo) ? signames.find(info->si_signo)->second.c_str() : "???      ", info->si_signo);    
    fprintf(out, " |  status:%10d", info->si_status);   
    fprintf(out, " |  int:%10d\n", info->si_int);   
    
    fprintf(out, "  errno:%4d", info->si_errno);    
    fprintf(out, " |  pid:%7d", info->si_pid);     
    fprintf(out, " |  utime:%11ld", info->si_utime);  
    fprintf(out, " |  overrun:%6d\n", info->si_overrun);  
    
    fprintf(out, "  code:%5d", info->si_code);     
    fprintf(out, " |  uid:%7d", info->si_uid);     
    fprintf(out, " |  stime:%11ld", info->si_stime);   
    fprintf(out, " |  timerid:%6d\n", info->si_timerid); 
    
    fprintf(out, "  lower:%14p", info->si_lower);
    fprintf(out, " |  call_addr:%14p", info->si_call_addr);
    fprintf(out, " |  band:%14ld", info->si_band);    
    fprintf(out, " |  pkey:%10d\n", info->si_pkey);   
    
    fprintf(out, "  upper:%14p", info->si_upper);                
    fprintf(out, " |  addr:%19p", info->si_addr); 
    fprintf(out, " |  addr_lsb:%10d", info->si_addr_lsb);
    fprintf(out, " |  fd:%12d\n", info->si_fd);    
    
    fprintf(out, "  syscall:%12d", info->si_syscall);
    fprintf(out, " |  arch:%10d", info->si_arch);
    fprintf(out, " |  value:%10d/%14p\n", info->si_value.sival_int, info->si_value.sival_ptr);   
    fflush(out);
} 

int main(int argc, char** argv){
    out = argc > 1 ? fopen(argv[1], "w") : stdout;
    if(!out) {
        printf("Could not open %s (%s)", argv[1], strerror(errno));
        return 1;
    }
    
    struct sigaction sa{};
    sa.sa_flags=SA_RESTART|SA_SIGINFO;
    sa.sa_sigaction=action;
    for(int i = 0 ;  i < 64; ++i)
        sigaction(i, &sa, nullptr);
    while(1) sleep(-1);
}

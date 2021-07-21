#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <signal.h>
#include <time.h>
#include <sys/time.h>   
#include <sys/stat.h>

timeval previous{};
long tx_bytes_prev,rx_bytes_prev;

char * tx_path;
char * rx_path; 

int cpu_cnt;
long ** cpu_now;
long * cpu_last;

void usr1(int) {
    FILE * file;
    long tx_bytes_now, rx_bytes_now;

    // take measurments

    timeval now;
    gettimeofday(&now, nullptr);

    file = fopen(tx_path,"r");
    fscanf(file, "%ld", &tx_bytes_now);
    fclose(file);

    file = fopen(rx_path,"r");
    fscanf(file, "%ld", &rx_bytes_now);
    fclose(file);

    file = fopen("/proc/stat","r");
    while(fgetc(file)!='\n');
    for(int i = 0 ; i < cpu_cnt; ++i) {
        char what[8];
        long * cpu = cpu_now[i];
        fscanf(file, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld",
               what, cpu, cpu+1, cpu+2, cpu+3, cpu+4, cpu+5, cpu+6, cpu+7, cpu+8, cpu+9);
    }
    
    // process and output
    
    auto usec = (now.tv_sec - previous.tv_sec)*1000000 + (now.tv_usec - previous.tv_usec);
    previous=now;

    printf("%ld.%06ld", now.tv_sec, now.tv_usec);

    
    float txrate = (tx_bytes_now - tx_bytes_prev) * 1000000.0 / usec;
    float rxrate = (rx_bytes_now - rx_bytes_prev) * 1000000.0 / usec;
    tx_bytes_prev=tx_bytes_now;
    rx_bytes_prev=rx_bytes_now;
    
    // printf("  TX: %12.2f  RX: %12.2f", txrate, rxrate);
    printf(" %.2f %.2f", txrate, rxrate);
    
    for(int c = 0 ; c < cpu_cnt; ++c) {
        long work = 0, total = 0;
        for(int i = 0; i < 10; ++i)
            total+=cpu_now[c][i];
        for(int i = 0; i < 3; ++i)
            work+=cpu_now[c][i];
        float usage = (work-cpu_last[2*c])*100./(total-cpu_last[2*c+1]);
    
        cpu_last[2*c] = work;
        cpu_last[2*c+1] = total;
    
        // printf("  CPU%d %5.2f%%", c, usage);
        printf(" %.2f", usage);
    }
    
    printf("\n");
    fflush(stdout);
}

int main(int argc, char ** argv) {
    if(argc != 3){
        printf("Wrong argument count!\n Usage: %s <ifname> <time_in_ms_from_10_to_999>\n", argv[0]);
        return -1;
    }
    long sampling_ms = atoi(argv[2]);
    long sampling_us = sampling_ms*1000l;
    long sampling_ns = sampling_us*1000l;
    
    rx_path = new char[strlen("/sys/class/net//statistics/rx_bytes")+strlen(argv[1])+1];
    tx_path = new char[strlen("/sys/class/net//statistics/tx_bytes")+strlen(argv[1])+1];
    sprintf(rx_path, "/sys/class/net/%s/statistics/rx_bytes", argv[1]);
    sprintf(tx_path, "/sys/class/net/%s/statistics/tx_bytes", argv[1]);
    
    struct stat st;
    
    if(stat(tx_path, &st) == -1 || stat(tx_path, &st) == -1 || sampling_ms < 10 || sampling_ms >= 1000){
        printf("Bad arguments!\n Usage: %s <ifname> <time_in_ms_from_10_to_999>\n", argv[0]);
        return -1;
    }
    
    cpu_cnt = -1;
    FILE * file = fopen("/proc/stat","r");
    while(true){
        char line[1024];
        fgets(line, 1024, file);
        if(strncmp(line, "cpu", 3)) break;
        cpu_cnt++;
    }
    
    cpu_now = new long*[cpu_cnt];
    for(int i = 0; i < cpu_cnt; ++i)
        cpu_now[i] = new long[10];
    cpu_last = new long[cpu_cnt*2];
    
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

    while(pause(),1);
}

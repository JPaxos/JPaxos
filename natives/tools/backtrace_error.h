#ifndef BACKTRACE_ERROR_H
#define BACKTRACE_ERROR_H

#ifndef __GNUC__
    #warning class "backtrace_error" falling back to "std::runtime_error" (reason: glibc not found)
    typedef std::runtime_error backtrace_error;
#else

#include <stdexcept>
#include <cstring>
#include <execinfo.h>
#include <errno.h>

#define MAX_TRACE_DEPTH 25

class backtrace_error : public std::runtime_error {
protected:
    std::string er;
    std::string bt;
    
    inline void getData(){
        er = strerror(errno);
        
        void * entries[MAX_TRACE_DEPTH];
        char **strings;
        int depth;
        
        depth = backtrace(entries, MAX_TRACE_DEPTH);
        if(depth == 0){
            bt = "could not generate stack trace (backtrace returned NULL)";
            return;
        }
        
        strings = backtrace_symbols(entries, depth);
        if(strings == nullptr){
            bt = "could not generate stack trace (backtrace_symbols returned nullptr)";
            return;
        }
        
        for(int i = 0 ; i < depth; ++i){
            bt += "\n";
            bt += std::to_string(i);
            bt += ": ";
            bt += strings[i];
        }

        free(strings);
    }
public:
    backtrace_error (const std::string& what_arg) : std::runtime_error(what_arg) {
        getData();
    }
    
    backtrace_error (const char* what_arg) : backtrace_error(std::string(what_arg)){}
    
    backtrace_error (const backtrace_error& other) noexcept : std::runtime_error(other) {
        er = other.er;
        bt = other.bt;
    }
    
    virtual ~backtrace_error(){}
    
    /** NOTICE: this function deliberately leaks memory (else Linux verbose terminate handler could not what()) */
    virtual const char* what() const noexcept override {
        std::string result;
        result = "\nReason: ";
        result += std::runtime_error::what();
        result += "\nErrno: ";
        result += er;
        result += "\nBacktrace: ";
        result += bt;
        result += "\nto make the trace look better, try:\n";
        result += R"(perl -e 'for(<>){/(\d+: )(\S+)\((\S+)\) (.*)/;my $a=`addr2line -Cpfse $2 $3`;chomp $a;print "$1$a $2($3) $4\n"}')";
        char * resultOnHeap = new char[result.length()+1];
        memcpy(resultOnHeap, result.c_str(), result.length()+1);
        return resultOnHeap;
    }
};

#endif
#endif

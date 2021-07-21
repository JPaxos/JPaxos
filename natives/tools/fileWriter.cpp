#include <unistd.h>
#include <fcntl.h>
#include <getopt.h>
#include <string>
#include <stdexcept>
#include <filesystem>
#include <sys/stat.h>

using namespace std::literals;
using std::stoul;

int main(int argc, char ** argv){
    char c;
    size_t size = 1024*1024*1024;
    ssize_t block = 4096;
    while( -1 !=  (c = getopt(argc, argv, "s:b:"))){
        switch(c){
            case 's':
                size = stoul(optarg);
                break;
            case 'b':
                block = stoul(optarg);
                break;
            default:
                throw std::invalid_argument("Unknown option "s + c);
        }
    }
    if(optind==argc)
        throw std::invalid_argument("Missing target filename/directory");
    if(optind!=argc-1)
        throw std::invalid_argument("Extra arguments");
    
    char * target = argv[optind];
    
    int fd;
    if(std::filesystem::is_directory(target))
        fd = open(target, O_EXCL|O_WRONLY|O_TMPFILE, 0666);
    else
        fd = open(target, O_EXCL|O_WRONLY|O_CREAT, 0666);
    if(fd==-1)
        throw std::filesystem::filesystem_error("Opening target file failed", target, std::error_code(errno, std::generic_category()));
    
    char * sourceBuffer = new char[block];
    
    size_t pos;
    for(pos = 0; pos < ((size/block)*block); pos += block){
        auto written = write(fd, sourceBuffer, block);
        if(written != block)
           throw std::filesystem::filesystem_error("Writing failed", target, std::error_code(errno, std::generic_category())); 
    }
    if(size%block){
        auto written = write(fd, sourceBuffer, size%block);
        if(written != (ssize_t)(size%block))
            throw std::filesystem::filesystem_error("Writing failed", target, std::error_code(errno, std::generic_category())); 
    }
    
    if(fsync(fd)==-1)
        throw std::filesystem::filesystem_error("Flushing failed", target, std::error_code(errno, std::generic_category()));
    
    if(close(fd)==-1)
        throw std::filesystem::filesystem_error("Closing failed", target, std::error_code(errno, std::generic_category()));
        
    return 0;
}

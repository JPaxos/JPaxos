#!/bin/bash
sysctl net.ipv4.tcp_mem='   65536   131072   262144'
sysctl net.ipv4.tcp_rmem=' 262144  4194304  8388608'
sysctl net.ipv4.tcp_wmem='  65536  1048576  2097152'
sysctl net.core.rmem_max=16777216
sysctl net.core.wmem_max=16777216

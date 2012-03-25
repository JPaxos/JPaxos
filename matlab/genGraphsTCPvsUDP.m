series = [[1,4096]];% [18,4096]];
%series = [[1,128]; [18,128]];
cd tcpvsudp
genGraphsTCPUDPXClients(series)
genGraphsTCPUDPReplicasXClients(series)
cd ..
function data = genModelData(sreq, sans, K, testbed)

N=3;

% Times in seconds, data in bytes
if strcmp(testbed, 'emulab')
    L=50;        % Latency (ms)    
    B=1232076;   % Bandwidth (bytes/sec) = 9.4 Mbit/s
elseif strcmp(testbed, 'cluster')
    L=0.1;     
    B=123207680; % Bandwidth 940Mbit/s
else
    disp(['Unknown testbed:' testbed])
end

s2b=8;     % Size of phase 2b messages
fprintf('Model: n=%d, B=%d, L=%d\n', N, B, L);

% Increased batching
data = [];
if strcmp(testbed, 'emulab')
    for k = K    
        [TauReqNet, ThrReqNet, TauReqCPU, ThrReqCPU, TauInstCPU, ThrInstCPU, TauInst, WND_CPU, WND_Net] ...
            = model_wan(N, B, L, sreq, k, s2b, sans);
        data = [data; sreq TauReqCPU ThrReqCPU TauReqNet ThrReqNet TauInstCPU ThrInstCPU TauInst WND_CPU WND_Net];
        %fprintf('sr=%d, ReqCPU=%2.2f,Thr=%2.2f; InstCPU=%2.2f,Thr=%2.2f; TInst=%2.2f; W_CPU=%2.2f; W_NET=%2.2f\n\n',...
    %        sreq, TauReqCPU, ThrReqCPU, TauInstCPU, ThrInstCPU, TauInst, WND_CPU, WND_Net);    
    end
else % Cluster
    for k = K    
        [TauReqNet, ThrReqNet, TauReqCPU, ThrReqCPU, TauInstCPU, ThrInstCPU, TauInst, WND_CPU, WND_Net] ...
            = model_lan(N, B, L, sreq, k, s2b, sans);
        data = [data; sreq TauReqCPU ThrReqCPU TauReqNet ThrReqNet TauInstCPU ThrInstCPU TauInst WND_CPU WND_Net];
        %fprintf('sr=%d, ReqCPU=%2.2f,Thr=%2.2f; InstCPU=%2.2f,Thr=%2.2f; TInst=%2.2f; W_CPU=%2.2f; W_NET=%2.2f\n\n',...
    %        sreq, TauReqCPU, ThrReqCPU, TauInstCPU, ThrInstCPU, TauInst, WND_CPU, WND_Net);    
    end    
end

%  1 sreq 
%  2 TauReqCPU 
%  3 ThrReqCPU 
%  4 TauReqNet 
%  5 ThrReqNet 
%  6 TauInstCPU 
%  7 ThrInstCPU 
%  8 TauInst 
%  9 WND_CPU 
% 10 WND_Net

n = length(K);
% fprintf('SR & k & cpu/req & T_req^cpu & net/req & T_req^net & cpu/inst & T_inst^cpu & rt/inst & CPU idle & net idle & W_CPU & W_NET & W\\\\\n\\hline\n');
% for i=1:n
%     fprintf('%3d & %3d & %2.2f & %8.2f & %4.2f & %8.2f & %4.2f & %4.2f & %4.2f & %4.2f & %4.2f & %4.2f & %5.2f & %4.2f \\\\\n', ...
%         data(i,1), K(i), data(i,2), data(i,3), data(i,4), data(i,5), data(i,6), data(i,7), data(i,8), (1-1/data(i,9)), (1-1/data(i,10)), data(i,9), data(i,10), min(data(i,9), data(i,10)))
% end

fprintf(' k & s2a & cpu/req & T_req^cpu & net/req & T_req^net & cpu/inst & T_inst^cpu & rt/inst & CPU idle & net idle & W_CPU & W_NET & W\\\\\n\\hline\n');
for i=1:n
    fprintf('%3d & %6d & %5.2f & %5.2f & %4.2f & %8.2f & %4.2f & %4.2f & %4.2f & %4.2f & %4.2f & %4.2f & %5.2f & %4.2f \\\\\n', ...
        K(i), sreq*K(i), data(i,2), data(i,3), data(i,4), data(i,5), data(i,6), data(i,7), data(i,8), (1-1/data(i,9)), (1-1/data(i,10)), data(i,9), data(i,10), min(data(i,9), data(i,10)))
end

% Shorter version for inclusion in the paper
fprintf(' s2a & cpu/req & cpu/inst & net/inst & rt/inst & W_CPU & W_NET \\\\\n\\hline\n');
for i=1:n
    batchSize = sreq*K(i);
    if batchSize > 1000
        batchStr = sprintf('%4.0fKB', batchSize/1024);
    else
        batchStr = sprintf('%4.0fB', batchSize);
    end
        
    fprintf('%6s & %4.2f & %4.2f & %4.2f & %4.2f & %4.2f & %5.2f \\\\\n', ...
        batchStr, data(i,2), data(i,6), data(i,4).*K(i), data(i,8), data(i,9), data(i,10))
end
% 
% Really short version
fprintf(' s2a & W_CPU & W_NET \\\\\n\\hline\n');
for i=1:n
    batchSize = sreq*K(i);
    if batchSize > 1000
        batchStr = sprintf('%4.0fKB', batchSize/1024);
    else
        batchStr = sprintf('%4.0fB', batchSize);
    end
        
    fprintf('%6s & %4.2f & %5.2f \\\\\n', ...
        batchStr, data(i,9), data(i,10))
end
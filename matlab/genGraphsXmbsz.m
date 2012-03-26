%Fix window size, vary batch size
%MBSZ = [1024 2048 4096 8192 16384 32768 65536 131072 262144 524288];

% dir = 'srds2011/results-emulab/rs128';
% WSZ = [1 2 5 10 20 30];
% ncli = 1200;
% MBSZ = [194 338 626 1202 2354 4658 9266 18482 36914 73778];
% genGraphsXmbszSeriesWnd(dir, MBSZ, WSZ, ncli);
% 
% dir = 'srds2011/results-emulab/rs1024';
% %WSZ = [1 2 5 10 15 20];
% WSZ = [1 2 5 10 20 30];
% ncli = 1200;
% MBSZ = [1340 2380 4460 8620 16940 33580 66860 133420 266540];
% genGraphsXmbszSeriesWnd(dir, MBSZ, WSZ, ncli);
% 
% dir = 'srds2011/results-emulab/rs8192';
% WSZ = [1 2 5 10 20 30];
% ncli = 1200;
% MBSZ = [8300 17000 35000 70000 140000 270000];
% genGraphsXmbszSeriesWnd(dir, MBSZ, WSZ, ncli);


dir = 'srds2011/results-cluster/rs128';
WSZ = [1 2 5];
MBSZ = [194 338 626 1202 2354 4658 9266 18482 36914 73778];
ncli = 900;
genGraphsXmbszSeriesWnd(dir, MBSZ, WSZ, ncli);

dir = 'srds2011/results-cluster/rs1024';
WSZ = [1 2 5];
MBSZ = [1340 2380 4460 8620 16940 33580 66860 133420 266540 532780];
ncli = 600;
genGraphsXmbszSeriesWnd(dir, MBSZ, WSZ, ncli);

dir = 'srds2011/results-cluster/rs8100';
WSZ = [1 2 5];
MBSZ = [8192 16384 32768 65536 131072 262144 524288];
ncli = 501;
genGraphsXmbszSeriesWnd(dir, MBSZ, WSZ, ncli);
 
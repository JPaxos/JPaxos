dir = 'srds2011/results-emulab/rs128';
WSZ = [1 2 5 10 15 20 25 30 35 40 45 50 55 60 65 70];
ncli = 1200;
%MBSZ = [194 338 626 1202 2354 4658 9266 18482 36914 73778];
MBSZ = [194 338 1202 4658 18482 73778];
genGraphsXalphaSeriesMBSZ(dir, WSZ, MBSZ, ncli);

% dir = 'srds2011/results-emulab/rs1024';
% WSZ = [1 2 5 10 15 20 30 40 50];
% ncli = 1200;
% % MBSZ = [1340 2380 4460 8620 16940 33580 66860 133420 266540];
% MBSZ = [1340 2380 8620 16940 33580 266540];
% genGraphsXalphaSeriesMBSZ(dir, WSZ, MBSZ, ncli);
% 
% dir = 'srds2011/results-emulab/rs8192';
% WSZ = [1 2 5 10 15 20 30];
% ncli = 1200;
% MBSZ = [8300 17000 35000 70000 140000 270000];
% genGraphsXalphaSeriesMBSZ(dir, WSZ, MBSZ, ncli);


% dir = 'srds2011/results-cluster/rs128';
% WSZ = [1 2 5];
% ncli = 900;
% MBSZ = [194 626 2354 9266 36914 73778];
% genGraphsXalphaSeriesMBSZ(dir, WSZ, MBSZ, ncli);
% 
% dir = 'srds2011/results-cluster/rs1024';
% WSZ = [1 2 5];
% ncli = 600;
% MBSZ = [1340 4460 16940 66860 266540 532780];
% genGraphsXalphaSeriesMBSZ(dir, WSZ, MBSZ, ncli);
% 
% dir = 'srds2011/results-cluster/rs8100';
% WSZ = [1 2 5];
% ncli = 501;
% MBSZ = [8192 16384 32768 131072 262144 524288];
% genGraphsXalphaSeriesMBSZ(dir, WSZ, MBSZ, ncli);

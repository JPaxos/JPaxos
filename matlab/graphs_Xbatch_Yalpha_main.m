sans=8;

K = [1 2 4 8 16 32 64 128 256 512];
sreq = 128;
graphs_Xbatch_Yalpha(sreq, sans, K, 'cluster');

K = [1 2 4 8 16 32 64 128 256 512];
sreq = 1024;
graphs_Xbatch_Yalpha(sreq, sans, K, 'cluster');

sreq = 8100;
K = [1 2 4 8 16 32 64];
graphs_Xbatch_Yalpha(sreq, sans, K, 'cluster');

% K = [1 2 4 8 16 32 64 128 256 512];
% sreq = 128;
% graphs_Xbatch_Yalpha(sreq, sans, K, 'emulab')
% 
% K = [1 2 4 8 16 32 64 128 256];
% sreq = 1024;
% graphs_Xbatch_Yalpha(sreq, sans, K, 'emulab')
% 
% sreq = 8192;
% K = [1 2 4 8 16 32];
% graphs_Xbatch_Yalpha(sreq, sans, K, 'emulab')
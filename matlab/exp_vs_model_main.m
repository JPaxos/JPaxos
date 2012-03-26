K = [1 2 4 8 16 32 64 128 256 512];
sreq = 128;
exp_vs_model(K, sreq, 'cluster')

K = [1 2 4 8 16 32 64 128 256 512];
sreq = 1024;
exp_vs_model(K, sreq, 'cluster')

sreq = 8100;
K = [1 2 4 8 16 32 64];
exp_vs_model(K, sreq, 'cluster')

% K = [1 2 4 8 16 32 64 128 256];
% sreq = 1024;
% exp_vs_model(K, sreq, 'emulab')
% 
% K = [1 2 4 8 16 32 64 128 256 512];
% sreq = 128;
% exp_vs_model(K, sreq, 'emulab')
% 
% sreq = 8192;
% K = [1 2 4 8 16 32];
% exp_vs_model(K, sreq, 'emulab')
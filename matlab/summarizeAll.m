function summarizeAll(directory)
% Enters all the subdirectories of results and for each subdirectory, does
% the following:
% - summarize the data on the consensus.txt file. creates a summary.txt
% file in each subdirectory with the mean and confidence intervals of the
% consensus latency and number of rounds per consensus
% - generates several plots showing the distribution of the data
%directory = 'results-cluster-rs110-c900';

% Save current working directory
old = cd(directory);

%old = cd('results-cluster');
% To find the analyse.m script
addpath(old)

% dirs = [dir('w_2_b_*'); dir('w_10_b_*')];
dirs = dir('w_*_b_*_c_*_rs_*');
for x = dirs'
    if ~x.isdir
        continue;
    end        
    testDir = x.name;

    disp(['Test directory: ' testDir]);
    cd(testDir);

    %if strcmp(subDir, 'fd_10') || strcmp(subDir, 'nonswift_10') || strcmp(subDir, 'swift_10')
    summarizeClients();
%    summarizeReplicas();
    cd ..
end
cd(old)

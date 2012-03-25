function allData = loadReplicaSummary()
%LOADREPLICASUMMARY Loads all the summary files
% Format of each line of the vector: [wsz bsz ncli <stats>]
allData = [];

dirs = dir('w_*_b_*_c_*_rs_*');
for x = dirs'
    if ~x.isdir
        continue;
    end

    subdir = x.name;

    % Extract the delay and loss probability from the name of the directory
    tokens = regexp(subdir, '_', 'split');
    wsz = str2double(tokens(2));
    bsz = str2double(tokens(4));
    ncli = str2double(tokens(6));
    reqsz = str2double(tokens(8));

    disp(['Entering ' subdir])
    cd(subdir)

    v = load('replicas.stats.txt');
    if isempty(v)
        disp('Skipping directory, no statistics file found')        
    else
        allData = [allData; wsz bsz ncli reqsz v];  
    end
    cd ..
end
allData = sortrows(allData, [1, 2, 3, 4]);
save('alldata.txt', 'allData', '-ASCII');
end

% 1 wsz 
% 2 bsz 
% 3 ncli
% 4 reqsz
% 5 #consensus 
% 6 time 
% 7 thrptInstances 
% 8 thrptRequests 
% 9 latency 
% 10 latencyci 
% 11 batchSz 
% 12 batchSzci 
% 13 totalRetrans 
% 14 avgRetrans

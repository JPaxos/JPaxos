function allData = loadClientSummary( )
%LOADCLIENTSUMMARY Summary of this function goes here
%   Detailed explanation goes here
allData = [];

dirs = dir('w_*_b_*_c_*');
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
   
    % sprintf('wsz=%d, bsz=%d, ncli=%d\n', wsz, bsz, ncli)

    disp(['Entering ' subdir])
    cd(subdir)    
    % #replicaID #req totalTime throughput latency latencyci
    v = load('clients.stats.txt');
    if isempty(v)
        disp('Skipping directory, no statistics file found')
    else
        % extract the line with the summary for all clients, 
        % represented by client id -1
        ind	= v(:,1) == -1;
        tmp = v(ind, 2:end);
        %[wsz bsz ncli tmp];
        allData = [allData; wsz bsz ncli reqsz tmp];    
    end
    cd ..
end

allData = sortrows(allData, [1, 2, 3, 4]);
save('clientData.txt', 'allData', '-ASCII');
end


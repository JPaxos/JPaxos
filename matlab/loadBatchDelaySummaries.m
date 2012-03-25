function allData = loadBatchDelaySummaries(file)
% Loads the summary files (allData.txt or clientData.txt) used to generate 
% the data with batchdelay.
% The batch delay is extracted from the name of the subdirectory and
% is added as first column of the resulting matrix.
allData = [];

dirs = dir('batchdelay_*');
for x = dirs'
    if ~x.isdir
        continue;
    end

    subdir = x.name;

    % Extract the delay and loss probability from the name of the directory
    tokens = regexp(subdir, '_', 'split');
    bdelay = str2double(tokens(2));
   
    % sprintf('wsz=%d, bsz=%d, ncli=%d\n', wsz, bsz, ncli)

    disp(['Entering ' subdir])
    cd(subdir)    
    % #replicaID #req totalTime throughput latency latencyci
    v = load(file);
    
    if isempty(v)
        disp('Skipping directory, no statistics file found')            
    end
        
    % Add the batch delay as first column of the data
    [m n]= size(v);
    bdcol = zeros(m,1)+bdelay;
    v = horzcat(bdcol, v);
    
    % concatenate the data of this directory to the list of
    % all data
    allData = vertcat(allData, v);
    cd ..
end

allData = sortrows(allData, [1 2 3 4]);


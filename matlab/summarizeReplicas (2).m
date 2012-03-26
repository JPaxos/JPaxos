function summarizeReplicas()

statsOutFile = 'replicas.stats.txt';
if ~isempty(dir(statsOutFile))
    disp 'Skipping directory'
    return
end

% Descriptive statistics of the data set
statsFD = fopen('replicas.stats.tex', 'w');
summaryFD = fopen(statsOutFile, 'w');
fprintf(summaryFD, '%% #consensus time thrptInstances thrptRequests latency latencyci batchSz batchSzci totalRetrans avgRetrans\n');


files = dir('replica-*.stats.log');
data = [];
for x = files'
    nData = load(x.name);
    data = [data; nData];
end


% WARNING: If the primary changes during the tests, the data from the replicas
% stats files has to be merged. Replicas log only the instances they decided,
% not the ones they started but didn't finish. Therefore, if the leader
% changes halfway through an instance, it will appear as having taken 
% only the time for the last replica to decide it, while in fact it took
% longer since it was first proposed by some replica. This has to be fixed.
data = sortrows(data, [1, 2]);
% This function removes the first 10% of the data
data = filterData(data);

% Normalize
baseTime = data(1,2);
data(:,2) = data(:,2)-baseTime;
if isempty(data)    
    fprintf(statsFD, 'Count:  & 0\n');
    fclose(statsFD);
    fclose(summaryFD);
    return;
end

% Columns
% 1 - instance
% 2 - Absolute start time
% 3 - duration
% 5 - ...

% Remove any instance with a negative duration. This indicates
% an instance that didn't complete at a replica because of leader
% change.
ind = data(:,3) > 0;
data = data(ind,:);

nInstances = length(data(:,1));
n = nInstances;
% Batch size column
nRequests = sum(data(:,4));
% Convert to milliseconds
%data(:,2) = data(:,2)/1000;
%data(:,3) = data(:,3)/1000;

totalTime=(data(n,2)+data(n,3)-data(1,2));
% multiply by 1000*1000 to convert from nanoseconds to seconds.
thrptInstances=1000*1000*(nInstances/totalTime);
thrptRequests=1000*1000*(nRequests/totalTime);

fprintf(statsFD, 'Count               : & %d \\\\\n', n);
fprintf(statsFD, 'TotalTime(real)     : & %d \\\\\n', totalTime);
fprintf(statsFD, 'Throughput Instances: & %d \\\\\n', thrptInstances);
fprintf(statsFD, 'Throughput Requests : & %d \\\\\n', thrptRequests);
% fprintf(statsFD, 'Latency:\n');
% fprintf(statsFD, '  Min:  & %d \\\\\n', min(data(:,4)));
% fprintf(statsFD, '  Max:  & %d \\\\\n', max(data(:,4)));
% fprintf(statsFD, '  Median:       & %2.2f \\\\\n', median(data(:,4)));
% fprintf(statsFD, '  Mean:         & %2.2f \\\\\n', latency);
% fprintf(statsFD, '  Std:          & %2.2f\n', latencyci);
% fprintf(statsFD, 'BatchSize:\n');
% fprintf(statsFD, '  Min:         & %d \\\\\n', min(data(:,5)));
% fprintf(statsFD, '  Max:         & %d \\\\\n', max(data(:,5)));
% fprintf(statsFD, '  Median:       & %2.2f \\\\\n', median(data(:,5)));
% fprintf(statsFD, '  Mean:         & %2.2f \\\\\n', mean(data(:,5)));
% fprintf(statsFD, '  Std:          & %2.2f\n', std(data(:,5)));
fclose(statsFD);


[muhat, sigmahat, muCI] = normfit(data(:,3));
% convert to milliseconds
latency = muhat/1000;
latencyci = (muhat-muCI(1))/1000;

[muhat, sigmahat, muCI] = normfit(data(:,4));
batchSize = muhat;
batchSizeci = muhat-muCI(1);

[muhat, sigmahat, muCI] = normfit(data(:,5));
valueSize = muhat;
valueSizeci = muhat-muCI(1);

[muhat, sigmahat, muCI] = normfit(data(:,6));
totalRetrans = sum(data(:,6));
avgRetrans = muhat;
avgRetransci = muhat-muCI(1);

[muhat, sigmahat, muCI] = normfit(data(:,7));
avgAlpha = muhat;
avgAlphaci = muhat-muCI(1);

fprintf(summaryFD, '%d\t%d\t%2.5f\t%2.5f\t%2.5f\t%2.5f\t%2.5f\t%2.5f\t%2.5f\t%2.5f\t%2d\t%2.5f\t%2.5f\t%2.5f\n', ...
    n, totalTime, thrptInstances, thrptRequests, latency, latencyci, ...
    batchSize, batchSizeci, valueSize, valueSizeci, ...
    totalRetrans, avgRetrans, avgAlpha, avgAlphaci);
fclose(summaryFD);

return
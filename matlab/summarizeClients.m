function summarizeClients()

% Parsing the results takes a while. The following avoids reparsing all 
% results when new results are added.
clientStatsFile = 'clients.stats.txt';
if ~isempty(dir(clientStatsFile))
    disp 'Skipping directory'
    return
end

summaryFD = fopen(clientStatsFile, 'w');
fprintf(summaryFD, '%% #replicaID #req totalTime throughput latency latencyci\n');

allDurations = [];
maxDuration = 0;
i = 0;
files = dir('client-*.stats.log');
for f = files'
    fid = fopen(f.name, 'r');
    if fid==-1
        break;
    end
    fclose(fid);
    %disp(['Opening ' name]);
    data = load(f.name);
    data = filterData(data);

    if isempty(data)
        disp(['Empty ' f.name])
        continue
    end
    
    % Normalize
    runStart = data(1,2);
    data(:,2) = data(:,2)-runStart;        
    allDurations = [allDurations; data(:,3)];
    
    [muhat, sigmahat, muCI] = normfit(data(:,3));
    latency = muhat;
    latencyci = muhat-muCI(1);
    n = length(data(:,1));
    duration = (data(n,2)+data(n,3))-data(1,2);
        
    % Scale from microseconds to milliseconds
    duration = duration/1000;
    latency = latency/1000;
    latencyci = latencyci/1000;
    
    maxDuration = max(maxDuration, duration);
    throughput = (1000*n)/maxDuration;    
    fprintf(summaryFD, '%d\t%d\t%d\t%2.5f\t%2.5f\t%2.5f\n', ...
        i, n, duration, throughput, latency, latencyci);
    i = i+1;
end

if isempty(allDurations)
    return;
end

[muhat, sigmahat, muCI] = normfit(allDurations);
latency = muhat;
latencyci = muhat-muCI(1);
% Scale from microseconds to milliseconds
latency = latency/1000;
latencyci = latencyci/1000;

n = length(allDurations);
throughput = (1000*n)/maxDuration;
fprintf(summaryFD, '%d\t%d\t%d\t%2.5f\t%2.5f\t%2.5f\n', ...
    -1, n, maxDuration, throughput, latency, latencyci);
fclose(summaryFD);

return

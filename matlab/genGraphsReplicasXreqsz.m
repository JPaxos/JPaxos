function genGraphsReplicasXreqsz(directory, REQSZ, mbsz, WSZ)
% Request size plotted on the x scale, with values taken from REQSZ
% mbsz is the batch size of the data plotted. 
% WSZ is a vector with the windows sizes used as data series

loadGraphSettings

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

% w b c reqsz 
allData = loadReplicaSummary();


[n cNodes testLength reqSize ] =  getTestDescription();
testDesc = sprintf('[n=%d, mbsz=%dKB, ncli=%d, d=%d]',...
    n, round(mbsz/1024), 100, testLength);

ind = ismember(allData(:,4), REQSZ );
allData = allData(ind, :);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Fix batch size, vary window size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Clear axes
cla


xlabelText = 'Request Size (KB) (ReqSZ)';
xlimits = [min(REQSZ)/1024 max(REQSZ)/1024];
allData(:,2) = allData(:,2)/1024;
allData(:,4) = allData(:,4)/1024;
% Plot only for the given range of clients
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);


legendString = {};
% latency
for i=1:length(WSZ)
    wsz = WSZ(i);
    
    %ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    ind	=  allData(:,1) == wsz;
    data = allData(ind, :);

	%errorbar(data(:,2), data(:,8), data(:,9), colors{i});    
    plot(data(:,4), data(:,9), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
    legendString = [legendString; ['w=' int2str(wsz)]];
end
xlabel(xlabelText);
ylabel('Latency (ms)');
xlim(xlimits);
legend(legendString, 'Location', 'NorthWest');
%title(['Instance Latency (Replica) ' testDesc]);
saveas(gcf, 'xreqsz-latency.eps', 'psc2');
hold off

cla
% Requests per instance
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz;
    data = allData(ind, :);
    % column 11 is the size of the batch, while column 4 is the size of the
    % request. Dividing the two gives approximately the number of requests
    % in a batch.
    % Convert first to KB, as column 4 is in KB as well.
    data(:,11) = data(:,11)/1024;
    plot(data(:,4), data(:,11)./data(:,4), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    %errorbar(data(:,4), data(:,11), data(:,12), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('#Requests');
xlim(xlimits);
%title(['Requests per instance ' testDesc]);
legend(legendString, 'Location', 'NorthWest');
saveas(gcf, 'xreqsz-nreqinstance.eps', 'psc2');
hold off


% Throughput instances
for i=1:length(WSZ)
    wsz = WSZ(i);
	ind	=  allData(:,1) == wsz;
    data = allData(ind, :);
    if largeScale
        plot(data(:,4), data(:,7)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,4), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
    hold on
end   
xlabel(xlabelText);
if largeScale
    ylabel('Instances/sec (x1000)');
else
    ylabel('Instances/sec');
end
% title(['Throughput instances ' testDesc]);
xlim(xlimits);
legend(legendString, 'Location', 'SouthWest');
saveas(gcf, 'xreqsz-throughput-instances.eps', 'psc2');
hold off

% Throughput in KB/s. How much data worth of requests are ordered.
% The actual data rate on the network is higher, as each request
% has to be transmitted to n-1 replicas and the reply has to be sent
% back to the client. 
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz;
    data = allData(ind, :);
    % .* element-wise operation
    if largeScale
        plot(data(:,4), (data(:,7).*data(:,11))/1024/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,4), (data(:,7).*data(:,11))/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
	hold on
end
xlabel(xlabelText);
% title(['Throughput data (decided) ' testDesc]);
xlim(xlimits);
legend(legendString, 'Location', 'SouthEast');
if largeScale
    ylabel('MB/sec');
    saveas(gcf, 'xreqsz-throughput-mbytes.eps', 'psc2');
else
    ylabel('KB/sec');
    saveas(gcf, 'xreqsz-throughput-kbytes.eps', 'psc2');
end
hold off

% Throughput requests
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz;
    data = allData(ind, :);
    if largeScale
        plot(data(:,4), data(:,8)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,4), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
    hold on
end
xlabel(xlabelText);
if largeScale
    ylabel('Requests/sec (x1000)');
else
    ylabel('Requests/sec');
end
% title(['Throughput requests ' testDesc]);
xlim(xlimits);
legend(legendString, 'Location', 'SouthEast');
hold off
saveas(gcf, 'xreqsz-throughput-requests.eps', 'psc2');

% Effective parallelism
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz;
    data = allData(ind, :);
    %errorbar(data(:,2), data(:,16), data(:,17), colors{i});
    plot(data(:,4), data(:,15), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('#Instances');
% title(['Concurrent instances ' testDesc]);
legend(legendString, 'Location', 'NorthWest');
hold off
saveas(gcf, 'xreqsz-concurrent-instances.eps', 'psc2');


cd(old)

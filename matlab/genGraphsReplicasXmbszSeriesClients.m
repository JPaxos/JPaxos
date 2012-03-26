function genGraphsReplicasXmbszSeriesClients(directory, wsz, MBSZ, CLIENTS)
% Alpha plotted on the x scale. WSZ are the values plotted on x scale
% mbsz is the batch size of the data plotted. 
% CLIENTS is a vector with the number of clients used as data series

loadGraphSettings

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

allData = loadReplicaSummary();


[n cNodes testLength reqSize ] =  getTestDescription();
testDesc = sprintf('[n=%d, reqSz=%dKB, \\alpha=%d, d=%d]',...
    n, round(reqSize/1024), wsz, testLength);
     

% Convert from bytes to KB
% allData(:,2) = allData(:,2)/1024;

ind = ismember(allData(:,2), MBSZ );
allData = allData(ind, :);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Fix batch size, vary window size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Clear axes
cla


xlabelText = 'Max. Batch Size (KB) (BSZ)';
xlimits = [min(MBSZ)/1024 max(MBSZ)/1024];
allData(:,2) = allData(:,2)/1024;
% Plot only for the given range of clients
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);


legendString = {};
% latency
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);    
    
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);

	%errorbar(data(:,2), data(:,8), data(:,9), colors{i});    
    plot(data(:,2), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
    % The tests on emulab were done with 99 and 501 clients to simplify
    % the script that was diving the clients over the 3 nodes available.
    % To avoid having to explain this number on the paper, we round it
    % 1 client out of 99 is a small difference.
    if ncli == 99
        ncli = 100;
    end
    if ncli == 501
        ncli = 500;
    end
    legendString = [legendString; ['cli=' int2str(ncli)]];
end
xlabel(xlabelText);
ylabel('Latency (ms)');
xlim(xlimits);
legend(legendString, 'Location', 'NorthWest');
%title(['Instance Latency (Replica) ' testDesc]);
saveas(gcf, 'xmbsz-latency.eps', 'psc2');
hold off

% Requests per instance
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    errorbar(data(:,2), data(:,10), data(:,11), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('#Requests');
xlim(xlimits);
%title(['Requests per instance ' testDesc]);
legend(legendString, 'Location', 'NorthWest');
saveas(gcf, 'xmbsz-nreqinstance.eps', 'psc2');
hold off

% Throughput instances
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
	ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    if largeScale
        plot(data(:,2), data(:,6)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,2), data(:,6), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
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
saveas(gcf, 'xmbsz-throughput-instances.eps', 'psc2');
hold off

% Throughput in KB/s. How many data worth of requests are ordered.
% The actual data rate on the network is higher, as each request
% has to be transmitted to n-1 replicas and the reply has to be sent
% back to the client. 
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    % .* element-wise operation
    if largeScale
        plot(data(:,2), (data(:,6).*data(:,12))/1024/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,2), (data(:,6).*data(:,12))/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
	hold on
end
xlabel(xlabelText);

% title(['Throughput data (decided) ' testDesc]);
xlim(xlimits);
legend(legendString, 'Location', 'SouthEast');
if largeScale
    ylabel('MB/sec');
    saveas(gcf, 'xmbsz-throughput-mbytes.eps', 'psc2');
else
    ylabel('KB/sec');
    saveas(gcf, 'xmbsz-throughput-kbytes.eps', 'psc2');
end
hold off

% Throughput requests
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    if largeScale
        plot(data(:,2), data(:,7)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,2), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
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
saveas(gcf, 'xmbsz-throughput-requests.eps', 'psc2');

% Effective parallelism
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    %errorbar(data(:,2), data(:,16), data(:,17), colors{i});
    plot(data(:,2), data(:,16), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('#Instances');
% title(['Concurrent instances ' testDesc]);
legend(legendString, 'Location', 'NorthWest');
hold off
saveas(gcf, 'xmbsz-concurrent-instances.eps', 'psc2');


cd(old)

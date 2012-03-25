function genGraphsReplicasXalpha(directory, WSZ, mbsz, CLIENTS)
% Alpha plotted on the x scale. WSZ are the values plotted on x scale
% mbsz is the batch size of the data plotted. 
% CLIENTS is a vector with the number of clients used as data series

% Script containing all the settings to control graphs
loadGraphSettings

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

allData = loadReplicaSummary();

[n cNodes testLength reqSize ] =  getTestDescription();
testDesc = sprintf('[n=%d, reqSz=%dKB, mbsz=%dKB, d=%d]',...
    n, round(reqSize/1024), round(mbsz/1024), testLength);
     

% Convert from bytes to KB
% allData(:,2) = allData(:,2)/1024;

ind = ismember(allData(:,1), WSZ );
allData = allData(ind, :);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Fix batch size, vary window size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Clear axes
cla
xlabelText = 'Window Size (WND)';
xlimits = [min(WSZ) max(WSZ)];
% Plot only for the given range of clients
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);

        
legendString = {};
% latency
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    
    
    if ncli == 99
        ncli = 100;
    end
    if ncli == 501
        ncli = 500;
    end
    
	%errorbar(data(:,1), data(:,8), data(:,9), colors{i});
    plot(data(:,1), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
    legendString = [legendString; ['cli=' int2str(ncli)]];
end
xlabel(xlabelText);
ylabel('Latency (ms)');
xlim(xlimits);
legend(legendString, 'Location', 'NorthWest');
%title(['Instance Latency (Replica)' testDesc]);
saveas(gcf, 'xalpha-latency.eps', 'psc2');
hold off

% Requests per instance
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    
    %errorbar(data(:,1), data(:,10), data(:,11), colors{i});
    plot(data(:,1), data(:,10), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('#Requests');
xlim(xlimits);
% title(testDesc);
legend(legendString, 'Location', 'NorthEast');
saveas(gcf, 'xalpha-nreqinstance.eps', 'psc2');
hold off

% Throughput instances
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    if largeScale
        plot(data(:,1), data(:,6)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,1), data(:,6), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
    hold on
end   
xlabel(xlabelText);
if largeScale
    ylabel('Instances/sec (x1000)');
else
    ylabel('Instances/sec');
end
% title(testDesc);
xlim(xlimits);
legend(legendString, 'Location', 'SouthEast');
saveas(gcf, 'xalpha-throughput-instances.eps', 'psc2');
hold off

% Throughput in KB/s
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    % .* element-wise operation
    if largeScale
        plot(data(:,1), (data(:,6).*data(:,12))/1024/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,1), (data(:,6).*data(:,12))/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
	hold on
end   
xlabel(xlabelText);
if largeScale
    ylabel('MB/sec');
else
    ylabel('KB/sec');
end
% title(testDesc);
xlim(xlimits);
legend(legendString, 'Location', 'NorthEast');
saveas(gcf, 'xalpha-throughput-mbytes.eps', 'psc2');
hold off

% Throughput requests
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    
    data(:,1), data(:,7)
    if largeScale
        plot(data(:,1), data(:,7)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,1), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
    hold on
end
xlabel(xlabelText);
if largeScale
    ylabel('Requests/sec (x1000)');
else
    ylabel('Requests/sec');
end
% title(testDesc);
xlim(xlimits);
legend(legendString, 'Location', 'SouthEast');
hold off
saveas(gcf, 'xalpha-throughput-requests.eps', 'psc2');

% Effective parallelism
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    
    plot(data(:,1), data(:,16), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    % errorbar(data(:,1), data(:,16), data(:,17), colors{i});
    hold on
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('#Instances');
% title(testDesc);
legend(legendString, 'Location', 'NorthWest');
hold off
saveas(gcf, 'xalpha-concurrent-instances.eps', 'psc2');


cd(old)

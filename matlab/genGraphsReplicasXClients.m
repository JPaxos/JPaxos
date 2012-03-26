function genGraphsReplicasXClients(directory, series)
% Generates graphs with increasing number of clients on the x scale
% WSZ and MBSZ are the lines that will be plotted

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

allData = loadReplicaSummary();

loadGraphSettings

[n cNodes testLength reqSize ] =  getTestDescription();
testDesc = sprintf('[n=%d, reqSz=%d, d=%d]', n, reqSize, testLength);
     

% Convert from bytes to KB
% allData(:,2) = allData(:,2)/1024;

%ind = ismember(allData(:,2), MBSZ );
%allData = allData(ind, :);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Fix window size, vary batch size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Clear axes
%cla reset
xlabelText = 'Number of clients';

% Plot only for the given range of clients
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);

        
legendString = {};
% latency
for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);
    %errorbar(data(:,3), data(:,8), data(:,9), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,3), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on    
    %legendString = [legendString; ['WND=' int2str(wsz) ', BSZ=' int2str(round(mbsz/1000)) 'KB']];
    legendString = [legendString; ['WND=' int2str(wsz)]];
end
xlabel(xlabelText);
ylabel('Latency (ms)');
tmp = xlim;
%xlim([0 tmp(2)]);
xlim([0 1000]);
legend(legendString, 'Location', 'NorthEast');
% title(['Instance Latency (Replica)' testDesc]);
saveas(gcf, 'xclients-latency.eps', 'psc2');
xlim([0 100]);
saveas(gcf, 'xclients-latency-detail.eps', 'psc2');
hold off


% Requests per instance
for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);
    %errorbar(data(:,3), data(:,10), data(:,11),'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,3), data(:,10), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('#Requests');
tmp = xlim;
%xlim([0 tmp(2)]);
xlim([0 1000]);
% title(testDesc);
legend(legendString, 'Location', 'NorthEast');
saveas(gcf, 'xclients-nreqinstance.eps', 'psc2');
hold off

% Size of each instance
for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);        
    %errorbar(data(:,3), data(:,12)/1024, data(:,13)/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,3), data(:,12)/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on    
end
xlabel(xlabelText);
ylabel('Size of proposal (KB)');
tmp = xlim;
%xlim([0 tmp(2)]);
xlim([0 1000]);
%title(['Requests per instance ' testDesc]);
legend(legendString, 'Location', 'NorthEast');
saveas(gcf, 'xclients-instancesize.eps', 'psc2');
hold off

% Throughput instances
for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);
    if largeScale
        plot(data(:,3), data(:,6)/1000,'LineStyle',  styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,3), data(:,6),'LineStyle',  styles{i}, 'Color', colors(i,:), 'Marker', markers(i));        
    end
    hold on
end   
xlabel(xlabelText);
if largeScale
    ylabel('Instances/sec (x1000)');
else
    ylabel('Instances/sec');
end
%title(['Throughput instances ' testDesc]);
xlim([0 max(data(:,3))]);
legend(legendString, 'Location', 'NorthEast');
saveas(gcf, 'xclients-throughput-instances.eps', 'psc2');
xlim([0 100]);
saveas(gcf, 'xclients-throughput-instances-detail.eps', 'psc2');
hold off

% Throughput in KB/s
for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);
    % .* element-wise operation
    plot(data(:,3), (data(:,6).*data(:,12))/(1024*1024), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end   
xlabel(xlabelText);
ylabel('MB/sec');
%title(['Throughput data (decided) ' testDesc]);
tmp = xlim;
xlim([0 tmp(2)]);
legend(legendString, 'Location', 'NorthEast');
saveas(gcf, 'xclients-throughput-kbytes.eps', 'psc2');
hold off

% Throughput requests
for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);        
    if largeScale
        plot(data(:,3), data(:,7)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    else
        plot(data(:,3), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    end
    hold on
end
xlabel(xlabelText);
if largeScale
    ylabel('Requests/sec (x1000)');
else
    ylabel('Requests/sec');
end
%title(['Throughput requests ' testDesc]);
legend(legendString, 'Location', 'NorthEast');
xlim([0 max(data(:,3))]);
saveas(gcf, 'xclients-throughput-requests.eps', 'psc2');
xlim([0 100]);
saveas(gcf, 'xclients-throughput-requests-detail.eps', 'psc2');
hold off


% Effective parallelism
for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);
    %errorbar(data(:,3), data(:,16)/1000, data(:,17), 'LineStyle',  styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,3), data(:,16), 'LineStyle',  styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('#Instances');
%title(['Concurrent instances ' testDesc]);
%xlim([0 tmp(2)]);
xlim([0 1000]);
legend(legendString, 'Location', 'NorthEast');
hold off
saveas(gcf, 'xclients-concurrent-instances.eps', 'psc2');


cd(old)

function genGraphsXmbszSeriesWnd(directory, MBSZ, WSZ, ncli)
% Alpha plotted on the x scale. WSZ are the values plotted on x scale
% mbsz is the batch size of the data plotted. 
% CLIENTS is a vector with the number of clients used as data series
loadGraphSettings

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

[n cNodes testLength reqSize ] =  getTestDescription();
testDesc = sprintf('[n=%d, reqSz=%dKB, d=%d]',...
    n, round(reqSize/1024), testLength);


allData = loadReplicaSummary();
ind = ismember(allData(:,2), MBSZ ) & allData(:,3) == ncli;
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
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    
	%errorbar(data(:,2), data(:,8), data(:,9), colors{i});    
    plot(data(:,2), data(:,9), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
    legendString = [legendString; ['WND=' int2str(wsz)]];
end
xlabel(xlabelText);
ylabel('Latency (ms)');
xlim(xlimits);
%y = ylim;
%ylim([0, 3000]);
%legend(legendString, 'Location', 'NorthWest');
%title(['Instance Latency (Replica) ' testDesc]);
saveas(gcf, sprintf('xmbsz_s_wnd_latency_rs%d.eps', reqSize), 'psc2');
hold off

cd old
return

% Requests per instance
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,2), data(:,11)/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    %plot(data(:,2), data(:,11)./data(:,4), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    %errorbar(data(:,2), data(:,11)./data(:,4), data(:,12)./data(:,4), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
%ylabel('#Requests');
ylabel('Batch Size (KB)');
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
%title(['Requests per instance ' testDesc]);
%legend(legendString, 'Location', 'NorthWest');
saveas(gcf, sprintf('xmbsz_s_wnd_nreqinstance_rs%d.eps', reqSize), 'psc2');
hold off

% Throughput instances, y-scale x1000
for i=1:length(WSZ)
    wsz = WSZ(i);
	ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,2), data(:,7)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end   
xlabel(xlabelText);
ylabel('Instances/sec (x1000)');
% title(['Throughput instances ' testDesc]);
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
%legend(legendString, 'Location', 'NorthEast');
saveas(gcf, sprintf('xmbsz_s_wnd_throughput-instances_x1000_rs%d.eps', reqSize), 'psc2');
hold off

% Throughput instances, normal y-scale
for i=1:length(WSZ)
    wsz = WSZ(i);
	ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,2), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end   
xlabel(xlabelText);
ylabel('Instances/sec');
% title(['Throughput instances ' testDesc]);
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
%legend(legendString, 'Location', 'NorthEast');
saveas(gcf, sprintf('xmbsz_s_wnd_throughput-instances_rs%d.eps', reqSize), 'psc2');
hold off


% Throughput in MB/s. How much data worth of requests are ordered.
% The actual data rate on the network is higher, as each request
% has to be transmitted to n-1 replicas and the reply has to be sent
% back to the client. 
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    % .* element-wise operation
    plot(data(:,2), (data(:,7).*data(:,11))/1024/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
end
xlabel(xlabelText);
% title(['Throughput data (decided) ' testDesc]);
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
%legend(legendString, 'Location', 'SouthEast');
ylabel('MB/sec');
saveas(gcf, sprintf('xmbsz_s_wnd_throughput-MB_rs%d.eps', reqSize), 'psc2');
hold off

% Throughput in KB.
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    % .* element-wise operation
    plot(data(:,2), (data(:,7).*data(:,11))/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
end
xlabel(xlabelText);
% title(['Throughput data (decided) ' testDesc]);
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
%legend(legendString, 'Location', 'SouthEast');
ylabel('KB/sec');
saveas(gcf, sprintf('xmbsz_s_wnd_throughput-KB_rs%d.eps', reqSize), 'psc2');
hold off


% Throughput requests
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,2), data(:,8)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('Requests/sec (x1000)');
% title(['Throughput requests ' testDesc]);
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
%legend(legendString, 'Location', 'SouthEast');
hold off
saveas(gcf, sprintf('xmbsz_s_wnd_throughput-requests_x1000_rs%d.eps', reqSize), 'psc2');


for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,2), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('Requests/sec');
% title(['Throughput requests ' testDesc]);
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
%if reqSize == 128
    %legend(legendString, 'Location', 'SouthEast');
%end
hold off
saveas(gcf, sprintf('xmbsz_s_wnd_throughput-requests_rs%d.eps', reqSize), 'psc2');


% Effective parallelism
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	=  allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    %errorbar(data(:,2), data(:,16), data(:,17), colors{i});
    plot(data(:,2), data(:,15), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
xlim(xlimits);
y = ylim;
ylim([0, y(2)]);
ylabel('#Instances');
% title(['Concurrent instances ' testDesc]);
%legend(legendString, 'Location', 'NorthWest');
hold off
saveas(gcf, sprintf('xmbsz_s_wnd_concurrent-instances_rs%d.eps', reqSize), 'psc2');




%%%%%%%%%%%%%%%%%%%%
% Client data
%%%%%%%%%%%%%%%%%%%%

allData = loadClientSummary();
ind = ismember(allData(:,2), MBSZ ) & allData(:,3) == ncli;

allData = allData(ind, :);
allData(:,2) = allData(:,2)/1024;

% Plot results only for a given range of clients.
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);

% Clear axes
cla

legendString = {};
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	= allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    
    %errorbar(data(:,2), data(:,8), data(:,8), colors{i});
    plot(data(:,2), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on        
end
xlabel(xlabelText);
xlim(xlimits);
if reqSize ~= 8192
    y = ylim;
    ylim([0, min(10000, y(2))]);    
end
ylabel('Latency (ms)');
% title(['Latency per request (client)\newline' testDesc]);
legend(legendString, 'Location', 'NorthEast');
hold off
saveas(gcf, sprintf('xmbsz_s_wnd_client-latency_rs%d.eps', reqSize), 'psc2');


% for i=1:length(WSZ)
%     wsz = WSZ(i);
%     ind	= allData(:,1) == wsz & allData(:,3) == ncli;
%     data = allData(ind, :);
%     plot(data(:,2), data(:,6), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
%     hold on
% end
% xlabel(xlabelText);
% xlim(xlimits);
% ylabel('Requests/sec');
% legend(legendString, 'Location', 'SouthEast');
% % title(['Throughput (Client)\newline' testDesc ]);
% hold off
% saveas(gcf, 'client-xmbsz-throughput.eps', 'psc2');


cd(old)

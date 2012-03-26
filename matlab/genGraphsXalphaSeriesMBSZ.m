function genGraphsXalphaSeriesMBSZ(directory, WSZ, MBSZ, ncli)
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
%testDesc = sprintf('[n=%d, reqSz=%dKB, mbsz=%dKB, d=%d]',...
%    n, round(reqSize/1024), round(mbsz/1024), testLength);

ind = ismember(allData(:,1), WSZ ) & allData(:,3) == ncli;
allData = allData(ind, :);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Fix batch size, vary window size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Clear axes
cla


xlabelText = 'Max. Window Size (WND)';
xlimits = [min(WSZ) max(WSZ)];
% Plot only for the given range of clients
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);

        
legendString = {};
% latency
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    

	%errorbar(data(:,1), data(:,8), data(:,9), colors{i});
    plot(data(:,1), data(:,9), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
    kmax = floor(mbsz / reqSize);
	kmax = pow2(floor(log2(kmax)));
    if mbsz < 1024
        legendString = [legendString; ['MBSZ=' int2str(mbsz)]];
    else
        legendString = [legendString; ['MBSZ=' int2str(mbsz/1024) 'KB']];
    end
end
xlabel(xlabelText);
ylabel('Latency (ms)');
xlim(xlimits);
%legend(legendString, 'Location', 'NorthWest');
%title(['Instance Latency (Replica)' testDesc]);
saveas(gcf, sprintf('xalpha_sbsz_latency_rs%d.eps',reqSize), 'psc2');
hold off

% Requests per instance
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    
    %errorbar(data(:,1), data(:,10), data(:,11), colors{i});
    plot(data(:,1), data(:,11)./data(:,4), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('#Requests');
xlim(xlimits);
% title(testDesc);
%legend(legendString, 'Location', 'NorthEast');
saveas(gcf, sprintf('xalpha_sbsz_nreqinstance_rs%d.eps',reqSize), 'psc2');
hold off

% Throughput instances
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,1), data(:,7)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end   
xlabel(xlabelText);
ylabel('Instances/sec (x1000)');
% title(testDesc);
xlim(xlimits);
%legend(legendString, 'Location', 'SouthEast');
saveas(gcf, sprintf('xalpha_sbsz_throughput-instances_x1000_rs%d.eps',reqSize), 'psc2');
hold off

% Throughput instances
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,1), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end   
xlabel(xlabelText);
ylabel('Instances/sec');
% title(testDesc);
xlim(xlimits);
%legend(legendString, 'Location', 'SouthEast');
saveas(gcf, sprintf('xalpha_sbsz_throughput-instances_rs%d.eps',reqSize), 'psc2');
hold off


% Throughput in KB/s
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    % .* element-wise operation
    plot(data(:,1), (data(:,7).*data(:,11))/1024/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
end   
xlabel(xlabelText);
ylabel('MB/sec');
% title(testDesc);
xlim(xlimits);
%legend(legendString, 'Location', 'NorthEast');
saveas(gcf, sprintf('xalpha_sbsz_throughput-MB_rs%d.eps',reqSize), 'psc2');
hold off

% Throughput in KB/s
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    % .* element-wise operation
    plot(data(:,1), (data(:,7).*data(:,11))/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on
end   
xlabel(xlabelText);
ylabel('KB/sec');
% title(testDesc);
xlim(xlimits);
%legend(legendString, 'Location', 'NorthEast');
saveas(gcf, sprintf('xalpha_sbsz_throughput-KB_rs%d.eps',reqSize), 'psc2');
hold off


% Throughput requests
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,1), data(:,8)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('Requests/sec (x1000)');
% title(testDesc);
xlim(xlimits);
%legend(legendString, 'Location', 'SouthEast');
hold off
saveas(gcf, sprintf('xalpha_sbsz_throughput-requests_x1000_rs%d.eps',reqSize), 'psc2');

for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    
    data(:,1), data(:,7)
    plot(data(:,1), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('Requests/sec');
% title(testDesc);
xlim(xlimits);
%legend(legendString, 'Location', 'SouthEast');
hold off
saveas(gcf, sprintf('xalpha_sbsz_throughput-requests_rs%d.eps',reqSize), 'psc2');


% Effective parallelism
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	=  allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);    
    plot(data(:,1), data(:,15), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('#Instances');
% title(testDesc);
%legend(legendString, 'Location', 'NorthWest');
hold off
saveas(gcf, sprintf('xalpha_sbsz_concurrent-instances_rs%d.eps',reqSize), 'psc2');
%%%%%%%%%%%%%%%%%%%%
% Client data
%%%%%%%%%%%%%%%%%%%%

allData = loadClientSummary();
ind = ismember(allData(:,1), WSZ ) & allData(:,3) == ncli;
allData = allData(ind, :);

% Plot results only for a given range of clients.
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);

% Clear axes
cla
for i=1:length(MBSZ)
    mbsz = MBSZ(i);
    ind	= allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    
    %errorbar(data(:,2), data(:,8), data(:,8), colors{i});
    plot(data(:,1), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
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
saveas(gcf, sprintf('xalpha_sbsz_client-latency_rs%d.eps', reqSize), 'psc2');


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

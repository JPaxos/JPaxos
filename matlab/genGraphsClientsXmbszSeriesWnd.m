function genGraphsClientsXmbszSeriesWnd(directory, MBSZ, WSZ, ncli)                    
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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Fix batch size, vary window size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


allData = loadClientSummary();
ind = ismember(allData(:,2), MBSZ ) & allData(:,3) == ncli;
allData = allData(ind, :);

% Clear axes
cla

xlabelText = 'Max. Batch Size (KB) (BSZ)';
xlimits = [min(MBSZ)/1024 max(MBSZ)/1024];
allData(:,2) = allData(:,2)/1024;

% Plot results only for a given range of clients.
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);


legendString = {};
for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	= allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    
    %errorbar(data(:,2), data(:,7), data(:,8), colors{i});
    plot(data(:,2), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on    
    legendString = [legendString; ['WND=' int2str(wsz)]];
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('Latency (ms)');
% title(['Latency per request (client)\newline' testDesc]);
legend(legendString, 'Location', 'NorthEast');
hold off
saveas(gcf, 'client-xmbsz-latency.eps', 'psc2');


for i=1:length(WSZ)
    wsz = WSZ(i);
    ind	= allData(:,1) == wsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,2), data(:,6), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('Requests/sec');
legend(legendString, 'Location', 'SouthEast');
% title(['Throughput (Client)\newline' testDesc ]);
hold off
saveas(gcf, 'client-xmbsz-throughput.eps', 'psc2');

cd(old)

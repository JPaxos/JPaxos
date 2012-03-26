function genGraphsClientsXalpha(directory, WSZ, mbsz, CLIENTS)
% Alpha plotted on the x scale. WSZ are the values plotted on x scale
% mbsz is the batch size of the data plotted. 
% CLIENTS is a vector with the number of clients used as data series
% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

loadGraphSettings

allData = loadClientSummary();

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

% Plot results only for a given range of clients.
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);

xlabelText = 'Window Size (WND)';
xlimits = [min(WSZ) max(WSZ)];

legendString = {};
for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	= allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    %errorbar(data(:,1), data(:,7), data(:,8), colors{i});
    plot(data(:,1), data(:,7), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
    
    if ncli == 99
        ncli = 100;
    end
    if ncli == 501
        ncli = 500;
    end
    legendString = [legendString; ['cli=' int2str(ncli)]];
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('Latency (ms)');
%title(['Latency per request (client)\newline' testDesc]);
legend(legendString, 'Location', 'NorthEast');
hold off
saveas(gcf, 'client-xalpha-latency.eps', 'psc2');


for i=1:length(CLIENTS)
    ncli = CLIENTS(i);
    ind	= allData(:,2) == mbsz & allData(:,3) == ncli;
    data = allData(ind, :);
    plot(data(:,1), data(:,6), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
xlim(xlimits);
ylabel('Requests/sec');
legend(legendString, 'Location', 'SouthEast');
%title(['Throughput (Client)\newline' testDesc ]);
hold off
saveas(gcf, 'client-xalpha-throughput.eps', 'psc2');

cd(old)

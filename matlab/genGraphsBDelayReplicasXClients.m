function genGraphsBDelayReplicasXClients(directory, wsz, mbsz)

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

allData = loadBatchDelaySummaries('allData.txt');
bdelays = sort(unique(allData(:,1)));

[ n cNodes testLength reqSize ] =  getTestDescription();
%testDesc = sprintf('[n=%d, reqSz=%d, d=%d, wsz=%d, mbsz=%d]',...
%         n, reqSize, testLength, wsz, mbsz);

loadGraphSettings
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Fix window size, vary batch size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Plot results only for a given range of clients.
%ind	=  allData(:,3) <= 250;
%allData = allData(ind, :);

% Latency
xlabelText = 'Number of clients';
legendString = {};

    
legendString = {};
% latency
for i = 1:length(bdelays)
    bdelay = bdelays(i);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == bdelay & allData(:,2) == wsz & allData(:,3) == mbsz;
    data = allData(ind, :);
        
    %errorbar(data(:,4), data(:,9), data(:,10), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,4), data(:,9), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on    
    if (bdelay < 0) 
        legendString = [legendString; ['TBN(' int2str(-bdelay) ')']];
    else
        legendString = [legendString; ['TB(' int2str(bdelay) ')']];
    end
    
end
xlabel(xlabelText);
ylabel('Latency (ms)');
xlim([0 max(data(:,4))]);
legend(legendString, 'Location', 'NorthEast');
%title(['Instance Latency (Replica)' testDesc]);
saveas(gcf, 'xclients-latency.eps', 'psc2');
hold off


% Requests per instance
for i = 1:length(bdelays)
    bdelay = bdelays(i);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == bdelay & allData(:,2) == wsz & allData(:,3) == mbsz;
    data = allData(ind, :);
    %errorbar(data(:,4), data(:,11), data(:,12),'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,4), data(:,11), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on    
end
xlabel(xlabelText);
ylabel('#Requests');
xlim([0 max(data(:,4))]);
%title(testDesc);
legend(legendString, 'Location', 'SouthEast');
saveas(gcf, 'xclients-nreqinstance.eps', 'psc2');
hold off

% Size of each instance
for i = 1:length(bdelays)
    bdelay = bdelays(i);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == bdelay & allData(:,2) == wsz & allData(:,3) == mbsz;
    data = allData(ind, :);
    %errorbar(data(:,4), data(:,13)/1024, data(:,14)/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,4), data(:,13)/1024, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on        
end
xlabel(xlabelText);
ylabel('Size of proposal (KB)');
xlim([0 max(data(:,4))]);
%title(testDesc);
legend(legendString, 'Location', 'SouthEast');
saveas(gcf, 'xclients-instancesize.eps', 'psc2');
hold off

% Throughput instances
for i = 1:length(bdelays)
    bdelay = bdelays(i);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == bdelay & allData(:,2) == wsz & allData(:,3) == mbsz;
    data = allData(ind, :);
    plot(data(:,4), data(:,7)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end   
xlabel(xlabelText);
ylabel('Instances/sec (x1000)');
%title(testDesc);
xlim([0 max(data(:,4))]);
legend(legendString, 'Location', 'NorthEast');
saveas(gcf, 'xclients-throughput-instances.eps', 'psc2');
hold off

% Throughput requests
for i = 1:length(bdelays)
    bdelay = bdelays(i);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == bdelay & allData(:,2) == wsz & allData(:,3) == mbsz;
    data = allData(ind, :);
	plot(data(:,4), data(:,8)/1000, 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
	hold on    
end
xlabel(xlabelText);
ylabel('Requests/sec (x1000)');
%title(testDesc);
xlim([0 max(data(:,4))]);
legend(legendString, 'Location', 'SouthEast');
hold off
saveas(gcf, 'xclients-throughput-requests.eps', 'psc2');

cd(old)

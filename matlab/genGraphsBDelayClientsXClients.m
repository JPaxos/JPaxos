function genGraphsBDelayClientsXClients(directory, wsz, mbsz)

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)

allData = loadBatchDelaySummaries('clientData.txt');
bdelays = sort(unique(allData(:,1)));

[ n cNodes testLength reqSize ] =  getTestDescription();
testDesc = sprintf('[n=%d, reqSz=%d, d=%d, wsz=%d, mbsz=%d]',...
         n, reqSize, testLength, wsz, mbsz);

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

for i = 1:length(bdelays)
    bdelay = bdelays(i);    
    if (bdelay < 0) 
        legendString = [legendString; ['TBN(' int2str(-bdelay) ')']];
    else
        legendString = [legendString; ['TB(' int2str(bdelay) ')']];
    end
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == bdelay & allData(:,2) == wsz & allData(:,3) == mbsz;
    data = allData(ind, :);        
    plot(data(:,4), data(:,8), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    %errorbar(data(:,4), data(:,8), data(:,9),  'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('Latency (ms)');
%title(['Latency per request (client)\newline' testDesc]);
xlim([0 max(data(:,4))]);
legend(legendString, 'Location', 'NorthEast');
hold off
saveas(gcf, 'client-xclients-latency.eps', 'psc2');


for i = 1:length(bdelays)
    bdelay = bdelays(i);    
    ind	=  allData(:,1) == bdelay & allData(:,2) == wsz & allData(:,3) == mbsz;
    data = allData(ind, :);       

    plot(data(:,4), data(:,5)/1000,  'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on    
end
xlabel(xlabelText);
ylabel('Requests/sec (x1000)');
legend(legendString, 'Location', 'SouthEast');
xlim([0 max(data(:,4))]);
%title(['Throughput (Client)\newline' testDesc ]);
hold off
saveas(gcf, 'client-xclients-throughput.eps', 'psc2');

cd(old)

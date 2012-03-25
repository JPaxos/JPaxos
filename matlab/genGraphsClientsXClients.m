function genGraphsClientsXClients(directory, series)

% Save current working directory
old = cd(directory);
% To find the analyse.m script
addpath(old)
allData = loadClientSummary();

[ n cNodes testLength reqSize ] =  getTestDescription();
testDesc = sprintf('[n=%d, reqSz=%d, d=%d]',...
         n, reqSize, testLength);

% Script containing all the settings to control graphs
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

for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);        
    %errorbar(data(:,3), data(:,7), data(:,8),  'LineStyle',  styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    plot(data(:,3), data(:,7), 'LineStyle',  styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
    %legendString = [legendString; ['WND=' int2str(wsz) ', BSZ=' int2str(round(mbsz/1000)) 'KB']];
    legendString = [legendString; ['WND=' int2str(wsz)]];
end
xlabel(xlabelText);
ylabel('Latency (ms)');
%title(['Latency per request (client)\newline' testDesc]);
tmp = xlim;
%xlim([0 tmp(2)]);
xlim([0 1000]);
legend(legendString, 'Location', 'NorthWest');
hold off
saveas(gcf, 'client-xclients-latency.eps', 'psc2');


for i = 1:size(series,1)
    wsz = series(i, 1);
    mbsz = series(i, 2);
    % retrieve the lines needed for the plot. That is, lines for tests
    % with wsz and for mbsz values equal to the current series being
    % generated
    ind	=  allData(:,1) == wsz & allData(:,2) == mbsz;
    data = allData(ind, :);       

    plot(data(:,3), data(:,6), 'LineStyle',  styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
end
xlabel(xlabelText);
ylabel('Requests/sec');
legend(legendString, 'Location', 'SouthEast');
tmp = xlim;
xlim([0 tmp(2)]);
%title(['Throughput (Client)\newline' testDesc ]);
hold off
saveas(gcf, 'client-xclients-throughput.eps', 'psc2');

cd(old)

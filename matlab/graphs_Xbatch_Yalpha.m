function graphs_Xbatch_Yalpha(sreq, sans, K, testbed)

addpath('analytical');
data = genModelData(sreq, sans, K, testbed);

% X-scale in KB/s
X = ((sreq+16).*K+4)/1024;

loadGraphSettings
% Plot optimal alpha as a function of batch size
cla reset
plot(X, data(:,9), 'LineStyle', styles{1}, 'Color', colors(1,:), 'Marker', markers(1));
hold on
plot(X, data(:,10), 'LineStyle', styles{2}, 'Color', colors(2,:), 'Marker', markers(2));
%title(sprintf('sreq=%d, sans=%d, n=3', sreq, sans));
xlabel('Batch Size (KB)');
y = ylim;
ylim([0 min(y(2), 40)]);
ylabel('Optimal W');
legend('W^{CPU}', 'W^{NET}', 'Location', 'NorthEast');
saveas(gcf, sprintf('model_Xbatch_Yalpha_%s_rs%d.eps',testbed, sreq), 'psc2');


cla reset
% Plot optimal alpha as a function of batch size
cla reset
plot(X, data(:,3), 'LineStyle', styles{1}, 'Color', colors(1,:), 'Marker', markers(1));
hold on
plot(X, data(:,5), 'LineStyle', styles{2}, 'Color', colors(2,:), 'Marker', markers(2));
plot(X, data(:,7), 'LineStyle', styles{3}, 'Color', colors(3,:), 'Marker', markers(3));
%title(sprintf('sreq=%d, sans=%d, n=3', sreq, sans));
xlabel('Batch Size (KB)');
ylabel('Throughput');
legend('CPU-Req', 'Net-Req', 'Instances', 'Location', 'NorthEast');
saveas(gcf, sprintf('model_Xbatch_Ythrp_%s_rs%d.eps',testbed, sreq), 'psc2');
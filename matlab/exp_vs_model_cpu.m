function exp_vs_model_cpu(K, sreq, testbed)

addpath('analytical');
loadGraphSettings

sans = 8;
data = genModelData(sreq, sans, K, testbed);

%file = sprintf('results-cluster-srds/rs%d/summary.txt',sreq);
% All results in a single file in the case of emulab
file = sprintf('results-%s-srds/rs%d/summary.txt', testbed, sreq);
disp(['Loading:' file]);
expdata = load(file);
ind = expdata(:,1) == 1 & expdata(:,3) == sreq;
expdata = expdata(ind, :);

% The x scale is the batch size in KB. Must convert from k, the number
% of requests per instance, to size in KB.
% Must transpose to form a vertical vector. Must match the data read from
% the data files.
X = (K*(sreq+16)/1024)';

% 16 bytes overhead per requests
%for i=1:n
%    fprintf('x=%4.2f, exp batch=%5.0f, model k: %d, exp k=%5.2f\n', ...
%        X(i), expdata(i,11), K(i), expdata(i,11)/(expdata(i,4)+16));
%end

%  9 - c-real
% 10 - c-cpu
% or 
% 10 - c-real
% 11 - c-cpu
n = length(K);

cla
%plot(X, data(1:n,8), 'LineStyle', styles{1}, 'Color', colors(1,:), 'Marker', markers(1));
%hold on
%plot(X, expdata(1:n,10), 'LineStyle', styles{2}, 'Color', colors(2,:), 'Marker', markers(2));
% Instances
plot(X, data(1:n, 6), 'LineStyle', styles{1}, 'Color', colors(1,:), 'Marker', markers(1));
hold on
plot(X, expdata(1:n,11), 'LineStyle', styles{2}, 'Color', colors(2,:), 'Marker', markers(2));
title(sprintf('sreq=%d, sans=%d, n=3', sreq, sans));
xlabel('Batch Size (KB)');
%xlim([0 64]);
ylabel('Time (ms)');
%legend('model-WCT', 'exp-WCT', 'model-CPU', 'exp-CPU', 'Location', 'NorthWest');
legend('model-CPU', 'exp-CPU', 'Location', 'NorthWest');
saveas(gcf, sprintf('exp_vs_model_cpu_%s_rs%d.eps',testbed, sreq), 'psc2');

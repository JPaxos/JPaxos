function exp_vs_model(K, sreq, testbed)

addpath('analytical');
loadGraphSettings

sans = 8;
% Increased batching
data = genModelData(sreq, sans, K, testbed);


%expdata = load('results-cluster-rs1024/alldata.txt');
%file = sprintf('results-cluster-srds/results-cluster-rs%d/alldata.txt', sreq);
file = sprintf('results-%s-srds/rs%d/summary.txt', testbed, sreq);
disp(['Loading:' file]);
% Use only results for w=1
expdata = load(file);
ind = expdata(:,1) == 1 & expdata(:,3) == sreq;
expdata = expdata(ind, :);

% The x scale is the batch size in KB. Must convert from k, the number
% of requests per instance, to size in KB.
X = (K*(sreq+16)/1024)';

n = length(K);
% 16 bytes overhead per requests
for i=1:n
    fprintf('x=%4.2f, exp batch=%5.0f, model k: %d, exp k=%5.2f\n', ...
        X(i), expdata(i,11), K(i), expdata(i,11)/(expdata(i,4)+16));
end

% Experimental data
%%%%%%%%%%%%%%%%%%%
% 1 wsz	
% 2 bsz	
% 3 rsz	
% 4 ncli
% 5 #consensus	
% 6 real	
% 7 cpu
% 8 user	
% 9 sys	
% 10 c-real
% 11 c-cpu
% 12 c-user
% 13 c-sys	
% 14 thrpInst	
% 15 thrpReq	
% 16 realOrder	
% 17 realInst

%modelThrpInst = 1000./data(1:n,8);


% Compare effective instance time (1000./expdata(1:n,7)) 
% with prediction of the model
cla
plot(X, data(1:n, 7), 'LineStyle', styles{1}, 'Color', colors(1,:), 'Marker', markers(1));
hold on
plot(X, expdata(1:n,14), 'LineStyle', styles{2}, 'Color', colors(2,:), 'Marker', markers(2));
% Instances
%plot(X, modelThrpInst.*K', 'LineStyle', styles{3}, 'Color', colors(3,:), 'Marker', markers(3));
plot(X, data(1:n,3), 'LineStyle', styles{3}, 'Color', colors(3,:), 'Marker', markers(3));
plot(X, expdata(1:n,15), 'LineStyle', styles{4}, 'Color', colors(4,:), 'Marker', markers(4));
title(sprintf('sreq=%d, sans=%d, n=3', sreq, sans));
xlabel('Batch Size (KB)');
%xlim([0 64]);
ylabel('Throughput');
legend('model-inst', 'exp-inst', 'model-req', 'exp-req', 'Location', 'SouthEast');
saveas(gcf, sprintf('exp_vs_model_thrp_%s_rs%d.eps',testbed, sreq), 'psc2');

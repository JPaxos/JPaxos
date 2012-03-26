function exp_vs_model_cpu_all(SREQ, testbed)

addpath('analytical');
loadGraphSettings
sans = 8;
set(gca,'FontSize',20);
cla reset
i = 1;
legendStr = {};
for sreq = SREQ
    if strcmp(testbed,'emulab')
         if sreq == 128 
            K = [1 2 4 8 16 32 64 128 256 512];
            reqSz='128';
        elseif sreq == 1024
            K = [1 2 4 8 16 32 64 128 256];
            reqSz='1KB';
        else
            K = [1 2 4 8 16 32];
            reqSz='8KB';
        end
    else
        if sreq == 128 
            K = [1 2 4 8 16 32 64 128 256 512];
            reqSz='128';
        elseif sreq == 1024
            K = [1 2 4 8 16 32 64 128 256 512];
            reqSz='1KB';
        else
            K = [1 2 4 8 16 32 64];
            reqSz='8KB';
        end
    end
    
    
    data = genModelData(sreq, sans, K, testbed);
    file = sprintf('results-%s-srds/rs%d/summary.txt', testbed, sreq);
    disp(['Loading:' file]);
    expdata = load(file);
    ind = expdata(:,1) == 1 & expdata(:,3) == sreq;
    expdata = expdata(ind, :);

    X = (K*(sreq+16)/1024)';

    n = length(K);

    plot(X, data(1:n, 6), 'LineStyle', styles{i}, 'Color', colors(i,:), 'Marker', markers(i));
    hold on
    plot(X, expdata(1:n,11), 'LineStyle', styles{i+1}, 'Color', colors(i+1,:), 'Marker', markers(i+1));   
    i = i+2;
    legendStr = [legendStr; ['model-' reqSz]];
    legendStr = [legendStr; ['exp-' reqSz]];    
end
xlabel('Batch Size (KB)'); 
ylabel('Time (ms)');
legend(legendStr, 'Location', 'SouthEast');
saveas(gcf, sprintf('exp_vs_model_cpu_%s.eps',testbed), 'psc2');

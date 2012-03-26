function summarizeThreadTimes(directory)

old = cd(directory);

allData = [];
dirs = dir('w_*_b_*_c_*_rs_*');
for x = dirs' 
    if ~x.isdir
        continue;
    end          
    testDir = x.name;
    cd(testDir);
    
    % Extract the delay and loss probability from the name of the directory
    tokens = regexp(testDir, '_', 'split');
    wsz = str2double(tokens(2));
    bsz = str2double(tokens(4));
    cli = str2double(tokens(6));
    rsz = str2double(tokens(8));
    
    % Open the statistics for the leader. It's the replica with the largest
    % stats.stats.log file.
    filename = '';
    maxsize = 0;
    files = dir('replica-*.stats.log');
    for f = files'
        if f.bytes > maxsize 
            filename = f.name;
            maxsize = f.bytes;
        end
    end
    
    leader=filename(9);
    
    %%%%%%%%%%%%%%%%%%%%%%%%
    % Start: Compute throughput
    %%%%%%%%%%%%%%%%%%%%%%%% 
    statsFile=['replica-' leader '.stats.log'];
    disp(['Opening ' testDir '/' statsFile]);    
    data = load(statsFile);
    data = filterData(data);    
    
    if ~isempty(data)
        % Remove any instance with a negative duration. This indicates
        % an instance that didn't complete at a replica because of leader
        % change or end of run
        ind = data(:,3) > 0;
        data = data(ind,:);
        
        % Number of instances
        n = length(data(:,1));
        % Normalize
        baseTime = data(1,2);
        data(:,2) = data(:,2)-baseTime;

        % Batch size column
        nRequests = sum(data(:,4));
        % Divide by 1000 to scale from micro to milliseconds
        totalTime=(data(n,2)+data(n,3)-data(1,2))/1000;
        % Scale from microseconds to milliseconds
        tInstOrder = normfit(data(:,3))/1000;
        % multiply by 1000 to convert from milliseconds to seconds.
        thrpInst=1000*(n/totalTime);
        thrpReq=1000*(nRequests/totalTime);    
    else
        disp(['Warning: empty file' statsFile]);
        thrpInst=-1;
        thrpReq=-1;
        tInstOrder=-1;
    end
    
    
    %%%%%%%%%%%%%%%%%%%%%%%%    
    % End: Compute throughput
    %%%%%%%%%%%%%%%%%%%%%%%%
       
    name = ['replica-' leader '-ThreadTimes.stats.log'];    
    disp(['Opening ' testDir '/' name]);
    data = load(name);
    if isempty(data)
        disp(['Warning: empty file' name]);
        consensus = -1;
        real = -1;
        user = -1;
        cpu = -1;
        % The average duration of an single instance. Assumes k=1
        tInstAll = -1;
    else
        n = length(data(:,1));
        consensus = data(n,1)-data(1,1);
        real = data(n,2);    
        user = data(n,3);
        cpu = data(n,4);
        % The average duration of an single instance. Assumes k=1
        tInstAll = real/consensus;        
    end
    allData = [allData; wsz bsz rsz cli consensus real cpu user thrpInst thrpReq tInstOrder tInstAll];
    
    cd ..
end

% Sort by window, batch
allData = sortrows(allData, [1 2 3 4]);
% Compute a few additional metrics and write the summary.
summaryFD = fopen('summary.txt', 'w');
fprintf(summaryFD, '%%wsz\tbsz\trsz\tcli\t#consensus\treal\tcpu\tuser\tsys\tc-real\tc-cpu\tc-user\tc-sys\tthrpInst\tthrpReq\trealOrder\trealInst\n');
for l=allData'    
    sys = l(7)-l(8);    
    tc_real = l(6)/l(5);
    tc_cpu = l(7)/l(5);
    tc_user = l(8)/l(5);
    tc_sys = sys/l(5);    
    fprintf(summaryFD, '%3d %6d %4d %4d %6d %6d %6d %6d %6d %6.2f %6.2f %6.2f %6.2f %6.1f %6.1f %7.2f %7.2f\n', ...
        l(1), l(2), l(3), l(4), l(5), l(6), l(7), l(8), sys, tc_real, tc_cpu, tc_user, tc_sys, l(9), l(10), l(11), l(12));
end
fclose(summaryFD);

cd(old)
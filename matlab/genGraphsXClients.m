% Scalability with number of clients
% WSZ = [1 2];
% MBSZ = [1024 64000];
% genGraphsReplicasXClients(dir, WSZ, MBSZ)
% genGraphsClientsXClients(dir, WSZ, MBSZ)
series = [[1,1024]; [5,1024]; [1,64000]];
dir = 'results-cluster-2010-09-17\cluster_reqsz1024_wx_ball';
genGraphsReplicasXClients(dir, series)
genGraphsClientsXClients(dir, series)
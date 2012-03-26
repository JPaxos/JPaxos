% Large requests
series = [[1,64000]; [2,64000]; [5,64000]];
dir = 'results-cluster-2010-09-17\reqsz63K-b64K';
genGraphsReplicasXClients(dir, series)
genGraphsClientsXClients(dir, series)
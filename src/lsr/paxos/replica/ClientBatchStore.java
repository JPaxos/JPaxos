package lsr.paxos.replica;

import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.ReplicaRequest;
import lsr.paxos.Paxos;
import lsr.paxos.statistics.QueueMonitor;

public final class ClientBatchStore {
    public final HashMap<Integer,RequestInfo>[] requests;
    public final int[] lower;
    public final int[] lowerProposed;
    public final int[] upper;
    
    /** 
     * Replicas exchange information about which requests they have
     * received from other replicas. Every replica keeps track of 
     * its knowledge about ACKs in the table below. 
     * 
     * Tracks the highest SN received by a given replica.
     * The row A[p] represents the information that the local replica
     * knows about replica p. A[p][q] is the highest sequence number
     * of a request sent by replica q, that p has received and acknowledged
     * to the local replica. 
     */
    public final int[][] rcvdUB;

    public final int f;
    public final int n;
    public final int localId;

    public ClientBatchStore() {
        this.n = ProcessDescriptor.getInstance().numReplicas;
        this.f = (n-1)/2;
        this.localId =  ProcessDescriptor.getInstance().localId;
        this.requests = (HashMap<Integer, RequestInfo>[]) new HashMap[n];
        for (int i = 0; i < n; i++) {
            requests[i] = new HashMap<Integer, RequestInfo>();
            QueueMonitor.getInstance().registerQueue("Replica-"+i+"-Forward", requests[i].values());
        }

        this.lower = new int[n];
        Arrays.fill(lower, 1);
        this.lowerProposed = new int[n];
        Arrays.fill(lowerProposed, 1);
        this.upper = new int[n];
        Arrays.fill(upper, 1);
        this.rcvdUB = new int[n][n];
        for (int i = 0; i < n; i++) {
            Arrays.fill(rcvdUB[i], 0);
        }
    }

    /** Local replica learned that replica r received request with rid */
    public void markReceived(int r, ReplicaRequestID rid) {
        assert rid.sn > rcvdUB[r][rid.replicaID] : 
            "FIFO order not preserved. Replica: " + r + ", HighestKnown: " + Arrays.toString(rcvdUB[rid.replicaID]) + ", next: " + rid.sn;
        int previous = rcvdUB[r][rid.replicaID];
        rcvdUB[r][rid.replicaID] = rid.sn;
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("New SN. Replica " + r + ", previous:" + previous + ", new: " + rid.sn + ", All: " + Arrays.toString(rcvdUB[localId]));
        }
    }

    /** Local replica learned that replica r received all requests from replica i up to rcvdUB[i] */
    public void markReceived(int r, int[] snUB) {
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Replica " + r + " acked up to " + Arrays.toString(snUB));
        }
        int[] v = rcvdUB[r];
//        logger.warning("Local: " + Arrays.toString(v) + ", New: " + Arrays.toString(snUB));
        for (int i = 0; i < n; i++) {
//            assert v[i] <= rcvdUB[i] : "Previous: " + Arrays.toString(v) + ", New: " + Arrays.toString(rcvdUB);
//            if (! (v[i] <= snUB[i])) {
//                logger.warning(i + " Previous: " + Arrays.toString(v) + ", New: " + Arrays.toString(snUB));
//            }
            // The leader receives both direct ACKs and the piggybacked updates, so it may 
            // increase
            v[i] = Math.max(v[i], snUB[i]);
        }
    }

    
    public void propose(Paxos paxos) throws InterruptedException {
        if (!paxos.isLeader()) {
            return;
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("[Start] lowerProposed: " + Arrays.toString(lowerProposed) + " - " + Arrays.toString(upper));
        }
        for (int i = 0; i < requests.length; i++) {
            HashMap<Integer, RequestInfo> m = requests[i];
            while (lowerProposed[i] < upper[i]) {
                int sn = lowerProposed[i];
                RequestInfo rInfo = m.get(sn);
                if (rInfo.state == State.NotProposed && rInfo.isStable()) {
                    if (!paxos.enqueueRequest(new ReplicaRequest(rInfo.rid))) {
                        
                    }
                } else {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("[" + i + "] Stopped proposing: " + rInfo + ", queueSize: " + m.size());
                    }
                    break;
                }
                lowerProposed[i]++;
            }
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("[end  ] lower: " + Arrays.toString(lowerProposed));
        }        
    }

    public void pruneLogs() {
        if (logger.isLoggable(Level.FINE)) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rcvdUB.length; i++) {
                sb.append(Arrays.toString(rcvdUB[i])).append("\n");
            }
            logger.fine("Prunning logs. rcvdUB:\n" + sb.toString());
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.info("[Start] lower: " + Arrays.toString(lower) + " - " + Arrays.toString(upper));
        }
        for (int i = 0; i < requests.length; i++) {
            HashMap<Integer, RequestInfo> m = requests[i];            
            while (lower[i] < upper[i]) {
                int sn = lower[i];
                RequestInfo rInfo = m.get(sn);
                if (rInfo.state == State.Executed && rInfo.allAcked()) {
                    m.remove(sn);
                } else {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("[" + i + "] Stopped prunning: " + rInfo + ", queueSize: " + m.size());
                    }
                    break;
                }
                lower[i]++;
            }
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.info("[end  ] lower: " + Arrays.toString(lower));
        }
    }

    public RequestInfo getRequestInfo(ReplicaRequestID rid) {
        return requests[rid.replicaID].get(rid.sn);
    }

    public void setRequestInfo(ReplicaRequestID rid, RequestInfo rInfo) {
        HashMap<Integer, RequestInfo> m = requests[rid.replicaID];
        assert !m.containsKey(rid.sn) : "Already contains request. Old: " + m.get(rid.sn) + ", Rcvd: " + rInfo;
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Initializing: " + rInfo.rid);
        }
        m.put(rid.sn, rInfo);
        assert upper[rid.replicaID] == rid.sn : "FIFO order violated. Old upper: " + upper[rid.replicaID] + ", new: " + rid.sn;
        upper[rid.replicaID] = rid.sn+1;
    }

    public boolean contains(ReplicaRequestID rid) {
        return requests[rid.replicaID].containsKey(rid.sn);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < requests.length; i++) {
            sb.append("[" + i + "] Size: " + requests[i].size());
            if (!requests[i].isEmpty()) {
//                sb.append(", Max: " + requests[i].lastKey() + " : " + requests[i]);
                sb.append(": " + requests[i]);
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public RequestInfo newRequestInfo(ReplicaRequestID id, ClientRequest[] batch) {
        return new RequestInfo(id, batch);
    }

    public RequestInfo newRequestInfo(ReplicaRequestID id) {
        return new RequestInfo(id);
    }

    enum State { NotProposed, Proposed, Decided, Executed };

    public final class RequestInfo {
        public final ReplicaRequestID rid; 
        public State state;
        public ClientRequest[] batch;

        RequestInfo(ReplicaRequestID id, ClientRequest[] batch) {
            this.batch = batch;
            this.rid = id;
            state=State.NotProposed;            
        }

        RequestInfo(ReplicaRequestID id) {
            this(id, null);
        }

        public boolean allAcked() {
            return countAcks() == n;
        }

        public boolean hasRequest(int replica) {
            int owner = rid.replicaID;
            return rcvdUB[replica][owner] >= rid.sn;
        }

        /** How many replicas have this request? */
        public int countAcks() {
            int rcvd = 0;
            for (int i = 0; i < n; i++) {
                if (hasRequest(i)) {
                    rcvd++;
                }
            }
            return rcvd;
        }

        public boolean isStable() {
            // Even if we don't have enough acks, if the request was decided
            // or executed it means that someone observed f+1 acks 
            if (state == State.Decided || state == State.Executed) {
                return true;
            }

            return countAcks() > f;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(
                    "rid:" + rid + ", Request: " + Arrays.toString(batch) + ", State:" + state + ", Acks: [");
            for (int i = 0; i < n; i++) {
                sb.append(hasRequest(i) ? '1' : '0');
            }
            sb.append("], stable:").append(isStable());
            return sb.toString();
        }
    }

    static final Logger logger = Logger.getLogger(ClientBatchStore.class.getCanonicalName());


}

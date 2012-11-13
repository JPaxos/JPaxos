package lsr.paxos.replica;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.paxos.core.Paxos;

public final class ClientBatchStore {

    /**
     * For each replica, keep a map with the request batches originating from
     * that replica. The requests are kept in the map until they are executed
     * and every other replica has acknowledged it.
     */
    // TODO: if a replica fails or becomes unresponsive, the map can grow
    // forever.
    // Must prune log even in this case. The unresponsive replica has to recover
    // from
    // a snapshot of the service state instead of replaying the log.
    public final HashMap<Integer, ClientBatchInfo>[] requests;
    // For replica i, the map above stores batches with ids between lower[i] and
    // upper[i].
    // There may be gaps when batches are received out of order.
    public final int[] lower;
    public final int[] upper;

    /*
     * firstNotProposed[i] is the smallest request batch from replica i that was
     * not yet proposed Only meaningful for the leader process, not used when in
     * follower role
     */
    public final int[] firstNotProposed;

    /**
     * Replicas exchange information about which requests they have received
     * from other replicas. Every replica keeps track of its knowledge about
     * ACKs in the table below.
     * 
     * Tracks the highest SN received by a given replica. The row A[p]
     * represents the information that the local replica knows about replica p.
     * A[p][q] is the highest sequence number of a request sent by replica q,
     * that p has received and acknowledged to the local replica.
     * 
     * NOTE: assumes that acknowledges are received sequentially
     */
    public final int[][] rcvdUB;

    // Caches the system parameters
    public final int f;
    public final int n;
    public final int localId;

    /*
     * When the protocol completes a view change at the Paxos level, it submits
     * a task to the ClientBatchManager thread to complete the view change at
     * the dissemination layer. This is executed asynchronously.
     * 
     * In the meantime, the protocol thread might do another view change, which
     * invalidates the previous view change at the dissemination layer.
     * 
     * Values: -1 - On follower mode x - Leader for view x
     */
    private int viewPrepared = -1;

    public ClientBatchStore() {
        this.n = ProcessDescriptor.getInstance().numReplicas;
        this.f = (n - 1) / 2;
        this.localId = ProcessDescriptor.getInstance().localId;
        this.requests = (HashMap<Integer, ClientBatchInfo>[]) new HashMap[n];
        for (int i = 0; i < n; i++) {
            requests[i] = new HashMap<Integer, ClientBatchInfo>(512);
        }

        this.lower = new int[n];
        Arrays.fill(lower, 1);
        this.firstNotProposed = new int[n];
        Arrays.fill(firstNotProposed, 1);
        this.upper = new int[n];
        Arrays.fill(upper, 1);
        this.rcvdUB = new int[n][n];
        for (int i = 0; i < n; i++) {
            Arrays.fill(rcvdUB[i], 0);
        }
    }

    /** Local replica learned that replica r received request with rid */
    public void markReceived(int r, ClientBatchID rid) {
        // The assumption behind rcvdUB is that replica r received from replica
        // p all requests
        // lower than rcvdUB[r][p]. Therefore, the requests must be acknowledged
        // sequentially.
        // However, it can receive an ack multiple times for the same request:
        // by receiving the message
        // with the batch directly from the sender or by receiving an ack from
        // another process.
        assert rid.sn <= rcvdUB[r][rid.replicaID] + 1 : "FIFO order not preserved. Replica: " + r +
                                                        ", HighestKnown: " +
                                                        Arrays.toString(rcvdUB[rid.replicaID]) +
                                                        ", next: " + rid.sn;
        int previous = rcvdUB[r][rid.replicaID];
        rcvdUB[r][rid.replicaID] = Math.max(rcvdUB[r][rid.replicaID], rid.sn);
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("New SN. Replica " + r + ", previous:" + previous + ", new: " + rid.sn +
                        ", All: " + Arrays.toString(rcvdUB[localId]));
        }
    }

    /**
     * Local replica learned that replica r received all requests from replica i
     * up to rcvdUB[i]
     */
    public void markReceived(int r, int[] snUB) {
        // Alias for the row of replica r
        int[] v = rcvdUB[r];
        // logger.warning("Local: " + Arrays.toString(v) + ", New: " +
        // Arrays.toString(snUB));
        for (int i = 0; i < n; i++) {
            // assert v[i] <= rcvdUB[i] : "Previous: " + Arrays.toString(v) +
            // ", New: " + Arrays.toString(rcvdUB);
            // if (! (v[i] <= snUB[i])) {
            // logger.warning(i + " Previous: " + Arrays.toString(v) + ", New: "
            // + Arrays.toString(snUB));
            // }
            // The leader receives both direct ACKs and the piggybacked updates,
            // so it may
            // increase
            v[i] = Math.max(v[i], snUB[i]);
        }
    }

    /**
     * Tries to propose more batches ids. A batch id can be proposed if it is
     * stable, ie, was acknowledged by f+1 or more other replicas.
     * 
     * @param paxos
     */
    public void propose(Paxos paxos) {
        if (paxos.getStorage().getView() != viewPrepared) {
            // Paxos has advanced view, cannot propose.
            return;
        }

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("[Start] " + limitsToString());
        }

        // For each replica, propose all batches of requests that are stable but
        // were not yet proposed
        for (int i = 0; i < requests.length; i++) {
            HashMap<Integer, ClientBatchInfo> m = requests[i];
            int sn = firstNotProposed[i];
            while (sn < upper[i]) {
                ClientBatchInfo bInfo = m.get(sn);
                // Reached a gap
                if (bInfo == null) {
                    logger.warning("Null batch info, gap reached. " + i + ":" + sn + ". State: " +
                                   limitsToString());
                    break;
                }
                if (bInfo.state == BatchState.NotProposed && bInfo.isStable()) {
                    if (logger.isLoggable(Level.INFO))
                        logger.info("Enqueuing batch: " + bInfo);
                    if (paxos.getStorage().getView() != viewPrepared ||
                        !paxos.enqueueRequest(bInfo.bid)) {
                        // This happens if while this propose() task is
                        // executing on the CliBatchManager thread,
                        // the Paxos layer executing on the Protocol thread
                        // changes view. The Protocol thread
                        // will enqueue a task on the CliBatchManager to disable
                        // it, but this task will take
                        // a while until being executed. In the meantime, we
                        // abort any propose task if we detect
                        // a change of view.
                        logger.warning("Failed to enqueue batch on Proposer: " + bInfo +
                                       ". viewPrepared: " + viewPrepared + ", Paxos view: " +
                                       paxos.getStorage().getView());
                        return;
                    } else {
                        bInfo.state = BatchState.Proposed;
                    }

                } else if (bInfo.state == BatchState.Executed ||
                           bInfo.state == BatchState.Decided || bInfo.state == BatchState.Proposed) {
                    // When the window size is greater than one, there there may
                    // be batches in the
                    // Decided, Executed or Proposed state interleaved with
                    // others in the NotProposed state.
                    // This may happen as a result of view change.
                    // Additionally, batches might become stable out-of-order,
                    // depends on the order
                    // of the delivery of acks. (Maybe not, because channels are
                    // FIFO?)
                    // Does not need to be proposed again. Skip this entry.
                    if (logger.isLoggable(Level.FINE))
                        logger.fine("Batch already proposed: " + bInfo);

                } else {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Reached a non-proposable batch: " + bInfo);// +
                                                                                // m.values().toString());
                    }
                    break;
                }
                sn++;
            }
            firstNotProposed[i] = sn;
        }
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("[End]" + limitsToString());
        }
    }

    /**
     * For every replica, delete all batches sent by that replica up to the
     * first batch that was not yet executed or not acked by all replicas.
     * 
     * This batch might still be needed, either for local execution or to
     * transmit to a different replica (not implemented).
     */
    public void pruneLogs() {
        if (logger.isLoggable(Level.FINE)) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rcvdUB.length; i++) {
                sb.append(Arrays.toString(rcvdUB[i])).append(" ");
            }
            logger.fine("Prunning logs. rcvdUB: " + sb.toString());
            logger.info(limitsToString());
        }

        // StringBuilder sb = new StringBuilder("Log size: ");

        for (int i = 0; i < requests.length; i++) {
            HashMap<Integer, ClientBatchInfo> m = requests[i];

            while (lower[i] < upper[i]) {
                int sn = lower[i];
                ClientBatchInfo rInfo = m.get(sn);
                // For tests with crashes only, limit size of log. Ugly hack.
                // if (rInfo != null && rInfo.state == BatchState.Executed)
                if (rInfo != null && rInfo.state == BatchState.Executed && rInfo.allAcked()) {
                    m.remove(sn);
                } else {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Stopped prunning at " + sn + ":" + rInfo +
                                    ", batches waiting: " + m.size());
                    }
                    break;
                }
                lower[i]++;
            }

            if (m.size() > 1000) {
                logger.warning(i + ": BatchStoreSize: " + m.size() + ", Limits: " +
                               limitsToString());
                logger.warning("Lowest bInfo: " + m.get(lower[i]));
            }
            // sb.append(i + ": " + m.size() + "; ");
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(limitsToString());
        }

    }

    /**
     * 
     * @param view The new view.
     * @param known The batch ids that were received during view change and are
     *            in the known state, so they will be proposed again by Paxos.
     * @param decided The batch ids that were received during view change and
     *            are on the Decided state.
     */
    public void onViewChange(int view, Set<ClientBatchID> known, Set<ClientBatchID> decided) {
        // Executed by the CliBatchManager thread
        if (logger.isLoggable(Level.INFO)) {
            logger.info("From Paxos log. Decided: " + decided + ", Known: " + known);
            logger.info("Before updating: " + limitsToString());
        }

        for (int i = 0; i < requests.length; i++) {
            HashMap<Integer, ClientBatchInfo> m = requests[i];
            // StringBuffer sb = new StringBuffer(i+ " ");
            for (ClientBatchInfo bInfo : m.values()) {
                // sb.append(", " + bInfo);
                if (logger.isLoggable(Level.FINEST))
                    logger.finest("Before: " + bInfo);
                if (bInfo.state == BatchState.Executed || bInfo.state == BatchState.Decided) {
                    // These batches do not need to be re-proposed, they were
                    // already decided.
                    if (known.contains(bInfo.bid)) {
                        // This can happen if a batch is ordered twice.
                        logger.info("Batch in known set was already decided or executed."
                                    + bInfo + ", " + limitsToString());
                    }
                    continue;
                } else {
                    // These batches may or may not have been proposed.
                    assert bInfo.state == BatchState.Proposed ||
                           bInfo.state == BatchState.NotProposed;
                    if (decided.contains(bInfo.bid)) {
                        logger.warning("Batch on decided set! " + bInfo);
                    }

                    // Did paxos become aware of the batch id during view
                    // change?
                    if (known.contains(bInfo.bid)) {
                        // Yes. Paxos is going to re-propose it.
                        bInfo.state = BatchState.Proposed;
                    } else {
                        // No. The ClientBatchManager must ask Paxos to propose
                        // the request again
                        bInfo.state = BatchState.NotProposed;
                    }
                }
                if (logger.isLoggable(Level.FINEST))
                    logger.finest("State after: " + bInfo.state);
            }
            // logger.warning(sb.toString());
        }

        // Reset firstNotProposed
        for (int i = 0; i < requests.length; i++) {
            HashMap<Integer, ClientBatchInfo> m = requests[i];
            int id = lower[i];
            while (id < upper[i]) {
                ClientBatchInfo bInfo = m.get(id);
                // Stop when either we don't have information about the batch or
                // the batch was not yet proposed
                if (bInfo == null || bInfo.state == BatchState.NotProposed) {
                    break;
                }
                // bInfo != null && bInfo.state != BatchState.NotProposed
                id++;
            }
            firstNotProposed[i] = id;
            // logger.warning("Stopped: " + ((id == upper[i]) ?
            // "Reached upper bound" : "" + m.get(id)));
        }
        if (logger.isLoggable(Level.INFO))
            logger.info("After updating: " + limitsToString());

        viewPrepared = view;
    }

    public ClientBatchInfo getRequestInfo(ClientBatchID rid) {
        return requests[rid.replicaID].get(rid.sn);
    }

    public void setRequestInfo(ClientBatchID rid, ClientBatchInfo rInfo) {
        HashMap<Integer, ClientBatchInfo> m = requests[rid.replicaID];
        assert !m.containsKey(rid.sn) : "Already contains request. Old: " + m.get(rid.sn) +
                                        ", Rcvd: " + rInfo;
        assert lower[rid.replicaID] <= rid.sn : "Request was already deleted. Current lower: " +
                                                lower[rid.replicaID] + ", request: " + rid;
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Initializing: " + rInfo.bid);
        }
        m.put(rid.sn, rInfo);
        // Since replicas use TCP to communicate among each other, the batches
        // must be received in order.
        // NOTE: This may be violated in the case of failures, if the connection
        // between replicas is interrupted.
        // In that case, a replica should have other indirect means of obtaining
        // the missing batches,
        // (like copying them from a third replica that did receive them), which
        // may result in violating
        // this FIFO order.
        // assert upper[rid.replicaID] == rid.sn :
        // "FIFO order violated. Old upper: " + upper[rid.replicaID] + ", new: "
        // + rid.sn;

        // TODO: Fifo order may be violated during view change.
        if (upper[rid.replicaID] != rid.sn) {
            logger.warning("FIFO order violated. " + rid + ". Old upper: " + upper[rid.replicaID] +
                           ", new: " + rid.sn);
        }
        upper[rid.replicaID] = Math.max(upper[rid.replicaID], rid.sn + 1);
    }

    public boolean contains(ClientBatchID rid) {
        return requests[rid.replicaID].containsKey(rid.sn);
    }

    public String limitsToString() {
        return "Lower: " + Arrays.toString(lower) +
               ", firstNotProposed: " + Arrays.toString(firstNotProposed) +
               ", upper: " + Arrays.toString(upper);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < requests.length; i++) {
            sb.append("[" + i + "] Size: " + requests[i].size());
            if (!requests[i].isEmpty()) {
                // sb.append(", Max: " + requests[i].lastKey() + " : " +
                // requests[i]);
                sb.append(": " + requests[i]);
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public ClientBatchInfo newRequestInfo(ClientBatchID id, ClientRequest[] batch) {
        return new ClientBatchInfo(id, batch);
    }

    public ClientBatchInfo newRequestInfo(ClientBatchID id) {
        return new ClientBatchInfo(id);
    }

    enum BatchState {
        NotProposed, Proposed, Decided, Executed
    };

    public final class ClientBatchInfo {
        public final ClientBatchID bid;
        public ClientRequest[] batch;
        // If the batch is local, the creation time of this instance is
        // approximately the
        // same as the time it is sent. Useful to measure the time from batch
        // forwarding to
        // batch decision.
        public final long timeStamp = System.currentTimeMillis();

        // Accessed by the ClientBatchManager (rw) and Replica thread (r).
        // As the replica thread only uses this variable for debugging, I'm not
        // setting it to volatile
        public BatchState state;

        ClientBatchInfo(ClientBatchID id, ClientRequest[] batch) {
            this.batch = batch;
            this.bid = id;
            state = BatchState.NotProposed;
        }

        ClientBatchInfo(ClientBatchID id) {
            this(id, null);
        }

        public boolean allAcked() {
            return countAcks() == n;
        }

        public boolean hasRequest(int replica) {
            int owner = bid.replicaID;
            return rcvdUB[replica][owner] >= bid.sn;
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
            if (state == BatchState.Decided || state == BatchState.Executed) {
                return true;
            }

            return countAcks() > f;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(128);
            sb.append("rid:").append(bid); // .append(", Request: ").append(Arrays.toString(batch));
            sb.append(", State:").append(state).append(", Acks: [");
            for (int i = 0; i < n; i++) {
                sb.append(hasRequest(i) ? '1' : '0');
            }
            sb.append("], stable:").append(isStable());
            return sb.toString();
        }
    }

    public int getLowerBound(int replicaID) {
        return lower[replicaID];
    }

    public void stopProposing() {
        this.viewPrepared = -1;
    }

    static final Logger logger = Logger.getLogger(ClientBatchStore.class.getCanonicalName());
}

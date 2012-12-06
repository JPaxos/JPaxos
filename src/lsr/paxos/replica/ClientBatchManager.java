package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Batcher;
import lsr.paxos.DecideCallback;
import lsr.paxos.core.Paxos;
import lsr.paxos.messages.AckForwardClientBatch;
import lsr.paxos.messages.ForwardClientBatch;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ClientBatchStore.BatchState;
import lsr.paxos.replica.ClientBatchStore.ClientBatchInfo;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.Storage;

/**
 * 
 * 
 * @author Nuno Santos (LSR)
 */
final public class ClientBatchManager implements MessageHandler, DecideCallback {

    private final SingleThreadDispatcher cliBManagerDispatcher;
    private final ClientBatchStore batchStore;

    /** Temporary storage for the instances that finished out of order. */
    private final Map<Integer, Deque<ClientBatchID>> decidedWaitingExecution =
            new HashMap<Integer, Deque<ClientBatchID>>();
    private int nextInstance;

    private final Network network;
    private final Paxos paxos;
    private final Replica replica;
    private final int localId;

    // Ack management
    private volatile long lastAckSentTS = -1;
    private volatile int[] lastAckedVector;

    // In milliseconds
    public final static String CLIENT_BATCH_ACK_TIMEOUT = "replica.ClientBatchAckTimeout";
    public final static int DEFAULT_CLIENT_BATCH_ACK_TIMEOUT = 50;
    private final int ackTimeout;

    // Thread that triggers an explicit ack when there are batches that were not
    // acked
    // and more than ackTimeout has elapsed since the last ack was sent.
    private final AckTrigger ackTrigger;

    // private final PerformanceLogger pLogger;

    public ClientBatchManager(Paxos paxos, Replica replica) {
        this.paxos = paxos;
        this.network = paxos.getNetwork();
        this.replica = replica;
        this.localId = processDescriptor.localId;
        this.cliBManagerDispatcher = new SingleThreadDispatcher("CliBatchManager");
        this.batchStore = new ClientBatchStore();
        this.ackTrigger = new AckTrigger();
        this.nextInstance = paxos.getStorage().getLog().getNextId();

        // Always clone the vector, or else the lastAckedVector will reference
        // the line
        // in the matrix that keeps the latest RID received, so the arrays will
        // always be
        // the same.
        this.lastAckedVector = batchStore.rcvdUB[localId].clone();
        this.lastAckSentTS = System.currentTimeMillis();
        this.ackTimeout = processDescriptor.config.getIntProperty(CLIENT_BATCH_ACK_TIMEOUT,
                DEFAULT_CLIENT_BATCH_ACK_TIMEOUT);

        // pLogger = PerformanceLogger.getLogger("replica-"+ localId+
        // "-CBatchManager");
        // pLogger.log("# BatchID   Time\n");
        logger.warning(CLIENT_BATCH_ACK_TIMEOUT + " = " + ackTimeout);
        Network.addMessageListener(MessageType.ForwardedClientRequest, this);
        Network.addMessageListener(MessageType.AckForwardedRequest, this);
        cliBManagerDispatcher.start();
        ackTrigger.start();
    }

    /* Handler for forwarded requests */
    @Override
    public void onMessageReceived(final Message msg, final int sender) {
        // Called by a TCP Sender
        cliBManagerDispatcher.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    if (msg instanceof ForwardClientBatch) {
                        onForwardClientBatch((ForwardClientBatch) msg, sender);

                    } else if (msg instanceof AckForwardClientBatch) {
                        onAckForwardClientBatch((AckForwardClientBatch) msg, sender);

                    } else {
                        throw new AssertionError("Unknown message type: " + msg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    @Override
    public void onMessageSent(Message message, BitSet destinations) {
        // Ignore
    }

    public ClientBatchStore getBatchStore() {
        return batchStore;
    }

    /**
     * Received a forwarded request.
     * 
     * @param fReq
     * @param sender
     * @throws InterruptedException
     */
    void onForwardClientBatch(ForwardClientBatch fReq, int sender)
    {
        assert cliBManagerDispatcher.amIInDispatcher() : "Not in ClientBatchManager dispatcher. " +
                                                         Thread.currentThread().getName();

        ClientRequest[] requests = fReq.requests;
        ClientBatchID rid = fReq.rid;

        ClientBatchInfo bInfo = batchStore.getRequestInfo(rid);
        // Create a new entry if none exists
        if (bInfo == null) {
            bInfo = batchStore.newRequestInfo(rid, requests);
            batchStore.setRequestInfo(rid, bInfo);
        } else {
            // The entry for the batch already exists. This happens when
            // received an ack or
            // a proposal for the request before receiving the request
            assert bInfo.batch == null : "Request already received. " + fReq;
            bInfo.batch = requests;
        }
        batchStore.markReceived(sender, rid);

        // The ForwardBatcher thread is responsible for sending the batch to
        // all,
        // including the local replica. The ClientBatchThread will then update
        // the
        // local data structures. If the ForwardedRequest comes from the local
        // replica, there is no need to send an acknowledge it to the other
        // replicas,
        // since when they receive the ForwardedBatch message they know that the
        // sender has the batch.
        if (sender != localId) {
            batchStore.markReceived(localId, rid);
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Received request: " + bInfo);
        }

        switch (bInfo.state) {
            case NotProposed:
                // tryPropose(rid);
                break;

            case Decided:
                executeRequests();
                break;

            case Executed:
                assert false : "Received a forwarded request but the request was already executed. Rcvd: " +
                               fReq + ", State: " + bInfo;

            default:
                throw new IllegalStateException("Invalid state: " + bInfo.state);
        }
        batchStore.markReceived(sender, fReq.rcvdUB);
        batchStore.propose(paxos);
    }

    void onAckForwardClientBatch(AckForwardClientBatch msg, int sender) throws InterruptedException {
        assert cliBManagerDispatcher.amIInDispatcher() : "Not in replica dispatcher. " +
                                                         Thread.currentThread().getName();
        if (logger.isLoggable(Level.FINE))
            logger.fine("Received ack from " + sender + ": " + Arrays.toString(msg.rcvdUB));
        batchStore.markReceived(sender, msg.rcvdUB);
        batchStore.propose(paxos);
    }

    /** Transmits a batch to the other replicas */
    public void sendNextBatch(ClientBatchID bid, ClientRequest[] batches) {
        assert cliBManagerDispatcher.amIInDispatcher();
        // Must make a copy of the vector.
        int[] ackVector = batchStore.rcvdUB[localId].clone();

        // The object that will be sent.
        ForwardClientBatch fReqMsg = new ForwardClientBatch(bid, batches, ackVector);

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Forwarding batch: " + fReqMsg);
        }

        markAcknowledged(ackVector);

        // Send to all
        network.sendToOthers(fReqMsg);
        // Local delivery
        onForwardClientBatch(fReqMsg, localId);
    }

    private void executeRequests() {
        assert cliBManagerDispatcher.amIInDispatcher() : "Not in replica dispatcher. " +
                                                         Thread.currentThread().getName();

        if (decidedWaitingExecution.size() > 100) {
            logger.warning("decidedWaitingExecution.size: " + decidedWaitingExecution.size() +
                           ", nextInstance: " + nextInstance +
                           ":" + decidedWaitingExecution.get(nextInstance));
        }
        if (logger.isLoggable(Level.INFO)) {
            logger.info("decidedWaitingExecution.size: " + decidedWaitingExecution.size());
        }

        while (true) {
            // Try to execute the next instance. It may not yet have been
            // decided.
            Deque<ClientBatchID> batch = decidedWaitingExecution.get(nextInstance);
            if (batch == null) {
                logger.info("Cannot continue execution. Next instance not decided: " + nextInstance);
                return;
            }

            logger.info("Executing instance: " + nextInstance);
            // execute all client batches that were decided in this instance.
            while (!batch.isEmpty()) {
                ClientBatchID bId = batch.getFirst();
                if (bId.isNop()) {
                    assert batch.size() == 1;
                    replica.executeNopInstance(nextInstance);

                } else {
                    // !bid.isNop()
                    ClientBatchInfo bInfo = batchStore.getRequestInfo(bId);
                    if (bInfo.batch == null) {
                        // Do not yet have the batch contents. Wait.
                        if (logger.isLoggable(Level.INFO)) {
                            logger.info("Request missing, suspending execution. rid: " + bInfo.bid);
                        }
                        for (int i = 0; i < batchStore.requests.length; i++) {
                            HashMap<Integer, ClientBatchInfo> m = batchStore.requests[i];
                            if (m.size() > 1024) {
                                logger.warning(i + ": " + m.get(batchStore.lower[i]));
                            }
                        }
                        return;
                    }

                    // bInfo.batch != null
                    if (logger.isLoggable(Level.FINE)) {
                        logger.info("Executing batch: " + bInfo.bid);
                    }
                    // execute the request, ie., pass the request to the Replica
                    // for execution.
                    bInfo.state = BatchState.Executed;
                    replica.executeClientBatch(nextInstance, bInfo);
                }
                batch.removeFirst();
            }
            // batch.isEmpty()
            // Done with all the client batches in this instance
            replica.instanceExecuted(nextInstance);
            decidedWaitingExecution.remove(nextInstance);
            nextInstance++;
        }
    }

    @Override
    public void onRequestOrdered(final int instance, final Deque<ClientBatchID> batch) {
        cliBManagerDispatcher.submit(new Runnable() {
            @Override
            public void run() {
                innerOnBatchOrdered(instance, batch);
            }
        });
    }

    /**
     * Called when the given instance was decided. Note: instances may be
     * decided out of order.
     * 
     * @param instance
     * @param batch
     */
    private void innerOnBatchOrdered(int instance, Deque<ClientBatchID> batch) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Instance: " + instance + ": " + batch.toString());
        }

        // Update the batch store, mark all client batches inside this Paxos
        // batch as decided.
        for (ClientBatchID bid : batch) {

            // NOP client batches should always be the only ClientBatch in a
            // Paxos batch.
            if (bid.isNop()) {
                assert batch.size() == 1 : "Found a NOP client batch in a batch with other requests: " +
                                           batch;
                continue;
            }

            // If the batch serial number is lower than lower bound, then
            // the batch was already executed and become stable.
            // This can happen during view change.
            if (bid.sn < batchStore.getLowerBound(bid.replicaID)) {
                if (logger.isLoggable(Level.INFO))
                    logger.info("Batch already decided (bInfo not found): " + bid);
                continue;
            }

            ClientBatchInfo bInfo = batchStore.getRequestInfo(bid);
            // Decision may be reached before having received the forwarded
            // request
            if (bInfo == null) {
                bInfo = batchStore.newRequestInfo(bid);
                batchStore.setRequestInfo(bid, bInfo);

            } else if (bInfo.state == BatchState.Decided || bInfo.state == BatchState.Executed) {
                // Already in the execution queue.
                if (logger.isLoggable(Level.INFO))
                    logger.info("Batch already decided. Ignoring. " + bInfo);
                continue;
            }

            bInfo.state = BatchState.Decided;
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Decided: " + bInfo.toString());
            }

            // pLogger.logln(rid + "\t" +
            // (System.currentTimeMillis()-rInfo.timeStamp));

            // if (logger.isLoggable(Level.INFO)) {
            // if (bid.replicaID == localId) {
            // if (logger.isLoggable(Level.INFO))
            // logger.info("Decided: " + bid + ", Time: " +
            // (System.currentTimeMillis()-bInfo.timeStamp));
            // }
            // }
        }
        // Add the Paxos batch to the list of batches that have to be executed.
        // Reorder buffer
        decidedWaitingExecution.put(instance, batch);
        executeRequests();
        batchStore.pruneLogs();
    }

    public void stopProposing() {
        cliBManagerDispatcher.submit(new Runnable() {
            @Override
            public void run() {
                batchStore.stopProposing();
            }
        });
    }

    /**
     * Called after view change is complete at the Paxos level, to enable the
     * ClientBatchManager. The ClientBatchManager updates its internal
     * datastructures based on what Paxos learned during view change. The goal
     * is to ensure that every batch id that was already proposed or decided is
     * not proposed again, and that every batch id that was not yet decided or
     * proposed is proposed. This is important to avoid duplicate orderings or
     * missed batches ids.
     * 
     * This prepares the ClientBatchManager to start issuing proposals of
     * batches ids to the Paxos layer.
     * 
     * @param view
     */
    public void startProposing(final int view) {
        // Executed in the Protocol thread. Accesses the paxos log.
        final Set<ClientBatchID> decided = new HashSet<ClientBatchID>();
        final Set<ClientBatchID> known = new HashSet<ClientBatchID>();

        Storage storage = paxos.getStorage();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("FirstNotCommitted: " + storage.getFirstUncommitted() + ", max: " +
                        storage.getLog().getNextId());
        }

        for (int i = storage.getFirstUncommitted(); i < storage.getLog().getNextId(); i++) {
            ConsensusInstance ci = storage.getLog().getInstance(i);
            if (ci.getValue() != null) {
                Deque<ClientBatchID> reqs = Batcher.unpack(ci.getValue());
                switch (ci.getState()) {
                    case DECIDED:
                        for (ClientBatchID replicaRequest : reqs) {
                            decided.add(replicaRequest);
                        }
                        break;

                    case KNOWN:
                        for (ClientBatchID replicaRequest : reqs) {
                            known.add(replicaRequest);
                        }
                        break;

                    case UNKNOWN:
                    default:
                        break;
                }
            }
        }
        // Should always be called from the Protocol thread.
        cliBManagerDispatcher.submit(new Runnable() {
            @Override
            public void run() {
                batchStore.onViewChange(view, known, decided);
            }
        });
    }

    public SingleThreadDispatcher getDispatcher() {
        return cliBManagerDispatcher;
    }

    /*------------------------------------------------------------
     * Ack management 
     *-----------------------------------------------------------*/

    /**
     * Task that periodically checks if the process should send explicit acks.
     * This happens if more than a given timeout elapsed since the last ack was
     * sent (either piggybacked or explicit) and if there are new batches that
     * have to be acknowledged.
     * 
     * This thread does not send the ack itself, instead it submits a task to
     * the dispatcher of the batch manager, which is the thread allowed to
     * manipulate the internal of the ClientBatch�Manager
     */
    class AckTrigger extends Thread {
        public AckTrigger() {
            super("CliBatchAckTrigger");
        }

        @Override
        public void run() {
            int sleepTime;
            while (true) {
                int delay = timeUntilNextAck();
                if (delay <= 0) {
                    // Execute the sendAcks in the dispatch thread.
                    cliBManagerDispatcher.submit(new Runnable() {
                        @Override
                        public void run() {
                            sendExplicitAcks();
                        }
                    });
                    sleepTime = ackTimeout;
                } else {
                    sleepTime = delay;
                }
                try {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Sleeping: " + sleepTime + ". lastAckedVector: " +
                                    Arrays.toString(lastAckedVector) + " at time " + lastAckSentTS);
                    }
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    logger.warning("Thread interrupted. Terminating");
                    return;
                }
            }
        }
    }

    /**
     * @return Time in milliseconds until the next round of explicit acks must
     *         be sent. A negative value indicates that the ack is overdue.
     */
    int timeUntilNextAck() {
        boolean allAcked = true;
        int[] rcvdUpperBound = batchStore.rcvdUB[localId];
        for (int i = 0; i < lastAckedVector.length; i++) {
            // Do not need to send acks for local batches. The message
            // containing
            // the batch is an implicit ack (the sender of the batch trivially
            // has the batch)
            if (i == localId)
                continue;
            allAcked &= (rcvdUpperBound[i] == lastAckedVector[i]);
        }

        if (allAcked) {
            // There is no need to send acks.
            return ackTimeout;
        }
        long now = System.currentTimeMillis();
        long nextSend = lastAckSentTS + ackTimeout;
        return (int) (nextSend - now);
    }

    /**
     * Called by the AckTrigger action to send explicit acks.
     */
    void sendExplicitAcks() {
        // Must recheck if it is still necessary to send acks. The state might
        // have changed since this task was submitted by the AckTrigger thread.
        int delay = timeUntilNextAck();
        if (delay <= 0) {
            int[] ackVector = batchStore.rcvdUB[localId].clone();
            // Send an ack to all other replicas.
            AckForwardClientBatch msg = new AckForwardClientBatch(ackVector);
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Ack time overdue: " + delay + ", msg: " + msg);
            }
            network.sendToOthers(msg);
            markAcknowledged(ackVector);
        }
    }

    /**
     * Updates the internal datastructure to reflect that an ack for the given
     * vector of batches id was just sent, either explicitly or piggybacked.
     * 
     * This method keeps a reference to the vector, so make sure it is not
     * modified after this method.
     * 
     * @param ackVector
     */
    private void markAcknowledged(int[] ackVector) {
        lastAckedVector = ackVector;
        lastAckSentTS = System.currentTimeMillis();
    }

    static final Logger logger = Logger.getLogger(ClientBatchManager.class.getCanonicalName());

    public void cleanStop() {
        cliBManagerDispatcher.shutdown();
        try {
            cliBManagerDispatcher.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

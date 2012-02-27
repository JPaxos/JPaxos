package lsr.paxos.replica;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientBatch;
import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.BatcherImpl;
import lsr.paxos.Paxos;
import lsr.paxos.messages.AckForwardClientBatch;
import lsr.paxos.messages.ForwardClientBatch;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ClientBatchStore.ClientBatchInfo;
import lsr.paxos.replica.ClientBatchStore.State;
import lsr.paxos.statistics.QueueMonitor;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.Storage;

final public class ClientBatchManager implements MessageHandler {

    private final SingleThreadDispatcher cliBManagerDispatcher;
    private final ClientBatchStore batchStore;
    /* May contain either an integer or a RequestInfo. 
     * An int i marks the end of the requests decided on batch i
     */ 
    private final Deque executionQueue = new ArrayDeque(256);
    private int currentInstance;

    private final Network network;
    private final Paxos paxos;
    private final Replica replica;
    private final int localId;

    private final BatcherImpl batcher = new BatcherImpl();

    // Ack management
    private volatile long lastAckSentTS = -1;
    private volatile int[] lastAckedVector; 

    // In milliseconds
    private final int ackTimeout = 50;
    // Thread that triggers an explicit ack when there are batches that were not acked 
    // and more than ackTimeout has elapsed since the last ack was sent.  
    private final AckTrigger ackTrigger;


    public ClientBatchManager(Paxos paxos, Replica replica){
        this.paxos = paxos;
        this.network = paxos.getNetwork();
        this.replica = replica;
        this.localId = ProcessDescriptor.getInstance().localId;
        this.cliBManagerDispatcher = new SingleThreadDispatcher("ClientBatchManager");
        this.batchStore = new ClientBatchStore();
        
        this.ackTrigger = new AckTrigger();
        // Always clone the vector, or else the lastAckedVector will reference the line
        // in the matrix that keeps the latest RID received, so the arrays will always be 
        // the same.
        this.lastAckedVector = batchStore.rcvdUB[localId].clone();
        this.lastAckSentTS = System.currentTimeMillis();
        
        Network.addMessageListener(MessageType.ForwardedClientRequest, this);
        Network.addMessageListener(MessageType.AckForwardedRequest, this);
        QueueMonitor.getInstance().registerQueue("ReqManExec", executionQueue);
        
        cliBManagerDispatcher.start();
        ackTrigger.start();
    }
    
    /* Handler for forwarded requests */
    @Override
    public void onMessageReceived(final Message msg, final int sender) {
        // Called by a TCP Sender         
        cliBManagerDispatcher.execute(new Runnable() {
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
        assert cliBManagerDispatcher.amIInDispatcher() :
            "Not in ClientBatchManager dispatcher. " + Thread.currentThread().getName();

        ClientRequest[] requests = fReq.requests;
        ClientBatchID rid = fReq.rid;

        ClientBatchInfo rInfo = batchStore.getRequestInfo(rid);
        // Create a new entry if none exists
        if (rInfo == null) {
            rInfo = batchStore.newRequestInfo(rid, requests);
            batchStore.setRequestInfo(rid, rInfo);
        } else {
            // The entry for the batch already exists. This happens when received an ack or 
            // a proposal for the request before receiving the request
            assert rInfo.batch == null : "Request already received. " + fReq;
            rInfo.batch = requests;           
        }        
        batchStore.markReceived(sender, rid);
        
        // The ForwardBatcher thread is responsible for sending the batch to all, 
        // including the local replica. The ClientBatchThread will then update the
        // local data structures. If the ForwardedRequest comes from the local
        // replica, there is no need to send an acknowledge it to the other replicas,
        // since when they receive the ForwardedBatch message they know that the
        // sender has the batch.
        if (sender != localId) {
            batchStore.markReceived(localId, rid);
        }
        
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Received request: " + rInfo);
        }
        
        switch (rInfo.state) {
            case NotProposed:
//                tryPropose(rid);
                break;

            case Decided:
                executeRequests();
                break;

            case Executed:
                assert false:
                    "Received a forwarded request but the request was already executed. Rcvd: " + fReq + ", State: " + rInfo;

            default:
                throw new IllegalStateException("Invalid state: " + rInfo.state);
        }
        batchStore.markReceived(sender, fReq.rcvdUB);
        batchStore.propose(paxos);
    }


    void onAckForwardClientBatch(AckForwardClientBatch msg, int sender) throws InterruptedException {
        assert cliBManagerDispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();
        if (logger.isLoggable(Level.FINE))
            logger.fine("Received ack from " + sender + ": " + Arrays.toString(msg.rcvdUB));
        batchStore.markReceived(sender, msg.rcvdUB);
        batchStore.propose(paxos);
    }


    private void executeRequests() {
        assert cliBManagerDispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ExecQueue.size: " + executionQueue.size());
        }
        while (!executionQueue.isEmpty()) {
            Object obj = executionQueue.peek();
            if (obj instanceof Integer) {
                // End of instance. Inform the replica. Required for snapshotting
                int instance = (Integer) obj;
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("End of instance: " + instance);
                }
                currentInstance = instance+1;
                replica.instanceExecuted(instance);
            } else {
                ClientBatchInfo rInfo = (ClientBatchInfo) obj;
                if (rInfo.batch == null) {
                    // Do not yet have the request. Wait.
                    if (logger.isLoggable(Level.INFO)) {
                        logger.info("Request missing, suspending execution. rid: " + rInfo.rid);
                    }
                    return;
                } else {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Executing batch: " + rInfo.rid);
                    }
                    // execute the request.
                    replica.executeClientBatch(currentInstance, rInfo.batch);
                    rInfo.state = State.Executed;
                }
            }
            // The request was handled. Remove it.
            executionQueue.pop();
        }
    }
    
    public void onBatchDecided(final int instance, final Deque<ClientBatch> batch) {
        cliBManagerDispatcher.execute(new Runnable() {
            @Override
            public void run() {
                innerOnBatchDecided(instance, batch);
            }
        });
    }

    void innerOnBatchDecided(int instance, Deque<ClientBatch> batch) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Batch decided. Instance: " + instance + ": " + batch.toString());
        }

        for (ClientBatch req : batch) {
            ClientBatchID rid = req.getRequestId();
            ClientBatchInfo rInfo = batchStore.getRequestInfo(rid);
            // Decision may be reached before having received the forwarded request
            if (rInfo == null) {
                rInfo = batchStore.newRequestInfo(req.getRequestId());
                batchStore.setRequestInfo(rid, rInfo);
            }
            rInfo.state = State.Decided;
            if (logger.isLoggable(Level.FINE)) {
                logger.fine(rInfo.toString());
            }
            executionQueue.add(rInfo);
        }
                
        // Place a marker to represent the end of the batch for this instance
        executionQueue.add(instance);
        executeRequests();
        batchStore.pruneLogs();
    }
    
    public void stopProposing() {
        batchStore.setProposerActive(false);        
    }
    
    public void startProposing() {
        // Executed in the Protocol thread. Accesses the paxos log.
        final Set<ClientBatchID> decided = new HashSet<ClientBatchID>();
        final Set<ClientBatchID> known = new HashSet<ClientBatchID>();

        Storage storage = paxos.getStorage();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("FirstNotCommitted: " + storage.getFirstUncommitted() + ", max: " + storage.getLog().getNextId());
        }
        
        for (int i = storage.getFirstUncommitted(); i < storage.getLog().getNextId(); i++) {            
            ConsensusInstance ci = storage.getLog().getInstance(i);
            if (ci.getValue() != null) {
                Deque<ClientBatch> reqs = batcher.unpack(ci.getValue());
                switch (ci.getState()) {
                    case DECIDED:
                        for (ClientBatch replicaRequest : reqs) {
                            decided.add(replicaRequest.getRequestId());
                        }
                        break;

                    case KNOWN:
                        for (ClientBatch replicaRequest : reqs) {
                            known.add(replicaRequest.getRequestId());
                        }
                        break;
                    case UNKNOWN:
                    default:
                        break;
                }
            }
        }
        
        cliBManagerDispatcher.submit(new Runnable() {
            @Override
            public void run() {
                batchStore.onViewChange(known, decided);
                batchStore.setProposerActive(true);
            }
        });
    }
    
    public SingleThreadDispatcher getDispatcher() {
        return cliBManagerDispatcher;
    }

    public void sendNextBatch(ClientBatchID bid, ClientRequest[] batches) {
        assert cliBManagerDispatcher.amIInDispatcher();
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

    /*------------------------------------------------------------
     * Ack management 
     *-----------------------------------------------------------*/
    
    /**
     * Task that periodically checks if the process should send explicit acks.
     * This happens if more than a given timeout elapsed since the last ack
     * was sent (either piggybacked or explicit) and if there are new batches
     * that have to be acknowledged. 
     * 
     *  This thread does not send the ack itself, instead it submits a task
     *  to the dispatcher of the batch manager, which is the thread allowed to
     *  manipulate the internal of the ClientBatch«Manager
     */
    class AckTrigger extends Thread {
        public AckTrigger() {
            super("ForwardedBatchesAckTrigger");
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
                        logger.fine("Sleeping: " + sleepTime + ". lastAckedVector: " + Arrays.toString(lastAckedVector) + " at time " + lastAckSentTS);
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
     * @return Time in milliseconds until the next round of explicit acks must be sent.
     * A negative value indicates that the ack is overdue.
     */ 
    int timeUntilNextAck() {
        boolean allAcked = true;
        int[] rcvdUpperBound = batchStore.rcvdUB[localId];
        for (int i = 0; i < lastAckedVector.length; i++) {
            // Do not need to send acks for local batches. The message containing
            // the batch is an implicit ack (the sender of the batch trivially has the batch)
            if (i==localId) continue;
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
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning("Ack time overdue: " + delay + ", msg: " + msg);
            }
            network.sendToOthers(msg);
            markAcknowledged(ackVector);
        }
    }

    /**
     * Updates the internal datastructure to reflect that an ack for the given 
     * vector of batches id was just sent, either explicitly or piggybacked. 
     * 
     * This method keeps a reference to the vector, so make sure it is 
     * not modified after this method.
     * 
     * @param ackVector
     */
    private void markAcknowledged(int[] ackVector) {
        lastAckedVector  = ackVector;
        lastAckSentTS = System.currentTimeMillis();
    }

    static final Logger logger = Logger.getLogger(ClientBatchManager.class.getCanonicalName());
}

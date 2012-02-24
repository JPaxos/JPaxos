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
import lsr.paxos.messages.AckForwardClientRequest;
import lsr.paxos.messages.ForwardClientRequest;
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
    private final ClientBatchStore batches;
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
    
    public ClientBatchManager(Paxos paxos, Replica replica){
        this.paxos = paxos;
        this.network = paxos.getNetwork();
        this.replica = replica;
        this.localId = ProcessDescriptor.getInstance().localId;
        this.cliBManagerDispatcher = new SingleThreadDispatcher("ClientBatchManager");
        this.batches = new ClientBatchStore();
        
        Network.addMessageListener(MessageType.ForwardedClientRequest, this);
        Network.addMessageListener(MessageType.AckForwardedRequest, this);
        QueueMonitor.getInstance().registerQueue("ReqManExec", executionQueue);
        
        cliBManagerDispatcher.start();
    }
    
    /* Handler for forwarded requests */
    @Override
    public void onMessageReceived(final Message msg, final int sender) {
        // Called by a TCP Sender         
        cliBManagerDispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (msg instanceof ForwardClientRequest) {
                        onForwardClientRequest((ForwardClientRequest) msg, sender);
                        
                    } else if (msg instanceof AckForwardClientRequest) {
                        onAckForwardClientRequest((AckForwardClientRequest) msg, sender);
                        
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
        return batches;
    }
    
    /** 
     * Received a forwarded request.
     *  
     * @param fReq
     * @param sender 
     * @throws InterruptedException 
     */
    void onForwardClientRequest(ForwardClientRequest fReq, int sender) throws InterruptedException 
    {
        assert cliBManagerDispatcher.amIInDispatcher() :
            "Not in ClientBatchManager dispatcher. " + Thread.currentThread().getName();

        ClientRequest[] requests = fReq.requests;
        ClientBatchID rid = fReq.rid;

        ClientBatchInfo rInfo = batches.getRequestInfo(rid);
        // Create a new entry if none exists
        if (rInfo == null) {
            rInfo = batches.newRequestInfo(rid, requests);
            batches.setRequestInfo(rid, rInfo);
        } else {
            // The entry for the batch already exists. This happens when received an ack or 
            // a proposal for the request before receiving the request
            assert rInfo.batch == null : "Request already received. " + fReq;
            rInfo.batch = requests;           
        }        
        batches.markReceived(sender, rid);
        
        // The ForwardBatcher thread is responsible for sending the batch to all, 
        // including the local replica. The ClientBatchThread will then update the
        // local data structures. If the ForwardedRequest comes from the local
        // replica, there is no need to send an acknowledge it to the other replicas,
        // since when they receive the ForwardedBatch message they know that the
        // sender has the batch.
        if (sender != localId) {
            batches.markReceived(localId, rid);
            // Send an ack to all other replicas.
            network.sendToOthers(new AckForwardClientRequest(batches.rcvdUB[localId]));
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
        batches.markReceived(sender, fReq.rcvdUB);
        batches.propose(paxos);
    }


    void onAckForwardClientRequest(AckForwardClientRequest msg, int sender) throws InterruptedException {
        assert cliBManagerDispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();
        if (logger.isLoggable(Level.FINE))
            logger.fine("Received ack from " + sender + ": " + Arrays.toString(msg.rcvdUB));
        batches.markReceived(sender, msg.rcvdUB);
        batches.propose(paxos);
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
                    if (logger.isLoggable(Level.INFO)) {
                        logger.info("Executing batch: " + rInfo.rid);
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
            ClientBatchInfo rInfo = batches.getRequestInfo(rid);
            // Decision may be reached before having received the forwarded request
            if (rInfo == null) {
                rInfo = batches.newRequestInfo(req.getRequestId());
                batches.setRequestInfo(rid, rInfo);
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
        batches.pruneLogs();
    }
    
    public void stopProposing() {
        batches.setProposerActive(false);        
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
                batches.onViewChange(known, decided);
                batches.setProposerActive(true);
            }
        });
    }

    static final Logger logger = Logger.getLogger(ClientBatchManager.class.getCanonicalName());
}

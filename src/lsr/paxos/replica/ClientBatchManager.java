package lsr.paxos.replica;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.ReplicaRequest;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Paxos;
import lsr.paxos.messages.ForwardClientRequest;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ClientBatchStore.RequestInfo;
import lsr.paxos.replica.ClientBatchStore.State;
import lsr.paxos.statistics.QueueMonitor;

public class ClientBatchManager implements MessageHandler {

    private final SingleThreadDispatcher dispatcher;
    private final ClientBatchStore batches;
    /* May contain either an integer or a RequestInfo. 
     * An int i marks the end of the requests decided on batch i
     */ 
    private final Deque executionQueue = new ArrayDeque(256);
    private int currentInstance;
    
    private final Paxos paxos;
    private final Replica replica;
    private final int localId;

    public ClientBatchManager(Paxos paxos, Replica replica){
        this.paxos = paxos;
        this.replica = replica;
        this.localId = ProcessDescriptor.getInstance().localId;
        this.dispatcher = new SingleThreadDispatcher("ClientBatchManager");
        this.batches = new ClientBatchStore();
        Network.addMessageListener(MessageType.ForwardedClientRequest, this);
        Network.addMessageListener(MessageType.AckForwardedRequest, this);

        QueueMonitor.getInstance().registerQueue("ReqManExec", executionQueue);
    }
    
    /* Handler for forwarded requests */
    @Override
    public void onMessageReceived(final Message msg, final int sender) {
        // Called by a TCP Sender         
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (msg instanceof ForwardClientRequest) {
                        onForwardClientRequest((ForwardClientRequest) msg, sender);
                    } else {
                        assert false;
//                        onAckForwardClientRequest((AckForwardClientRequest) msg, sender);
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
    private void onForwardClientRequest(ForwardClientRequest fReq, int sender) throws InterruptedException 
    {
        assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

        ClientRequest[] requests = fReq.requests;
        ReplicaRequestID rid = fReq.rid;

        RequestInfo rInfo = batches.getRequestInfo(rid);
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
        if (sender != localId) {
            batches.markReceived(localId, rid);
        }
        
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Received request: " + rInfo);
        }        
//        int leader = paxos.getLeaderId();
//        if (localId != leader) {
//            network.sendMessage(new AckForwardClientRequest(rid), leader);
//        }
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

    
    private void executeRequests() {
        assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

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
                RequestInfo rInfo = (RequestInfo) obj;
                if (rInfo.batch == null) {
                    // Do not yet have the request. Wait.
                    if (logger.isLoggable(Level.INFO)) {
                        logger.info("Request missing, suspending execution. rid: " + rInfo.rid);
                    }
                    return;
                } else {
//                    if (logger.isLoggable(Level.INFO)) {
//                        logger.info("Executing. rid: " + rInfo.rid + ". cid: " + rInfo.request.getRequestId());
//                    }
                    // execute the request.
                    replica.executeClientBatch(currentInstance, rInfo.batch);
                    rInfo.state = State.Executed;
                }
            }
            // The request was handled. Remove it.
            executionQueue.pop();
        }
    }
    
    public void onBatchDecided(final int instance, final Deque<ReplicaRequest> batch) {
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                innerOnBatcheDecided(instance, batch);
            }
        });
    }
    
    void innerOnBatcheDecided(int instance, Deque<ReplicaRequest> batch) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Batch decided. Instance: " + instance + ": " + batch.toString());
        }
        for (ReplicaRequest req : batch) {
            ReplicaRequestID rid = req.getRequestId();
            RequestInfo rInfo = batches.getRequestInfo(rid);
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


//  public void onAckForwardClientRequest(AckForwardClientRequest ack, int sender) throws InterruptedException 
//  {
//      assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();
//
//      ReplicaRequestID rid = ack.id;
//      RequestInfo rInfo = requests.getRequestInfo(rid);
//      if (rInfo == null) {
//          rInfo = requests.newRequestInfo(rid);
//          requests.setRequestInfo(rid, rInfo);
//      }
//      requests.markReceived(sender, rid);
//      if (logger.isLoggable(Level.FINE)) {
//          logger.fine("Ack from " + sender + " for " + rInfo);
//      }
//      tryPropose(rid);
//  }

    static final Logger logger = Logger.getLogger(ClientBatchManager.class.getCanonicalName());
}

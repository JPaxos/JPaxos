package lsr.paxos.replica;

import java.io.IOException;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.ClientRequest;
import lsr.common.ReplicaRequest;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.common.nio.SelectorThread;
import lsr.paxos.Paxos;
import lsr.paxos.statistics.QueueMonitor;

/**
 * This class handles all commands from the clients. A single instance is used
 * to manage all clients.
 * 
 */
public class ClientRequestManager {

    final Replica replica;
    final Paxos paxos;

    /*
     * Threading This class is accessed by several threads: 
     * - the SelectorThreads that read the requests from the clients: method execute() 
     * - the Replica thread after executing a request: method handleReply()
     * 
     * The maps pendingClientProxies and lastReplies are accessed by the thread
     * reading requests from clients and by the replica thread.
     */

    /* Flow control: bound on the number of requests that can be in the system, waiting
     * to be ordered and executed. When this limit is reached, the selector threads will 
     * block on pendingRequestSem.
     */    
    private static final int MAX_PENDING_REQUESTS = 1024;
    private final Semaphore pendingRequestsSem = new Semaphore(MAX_PENDING_REQUESTS);

    /**
     * Requests received but waiting ordering. request id -> client proxy
     * waiting for the reply. Accessed by Replica and Selector threads.
     */
    private final Map<RequestId, NioClientProxy> pendingClientProxies =
            new ConcurrentHashMap<RequestId, NioClientProxy>((int)(MAX_PENDING_REQUESTS*1.5), (float) 0.75, 8);
    
    /**
     * Keeps the last reply for each client. Necessary for retransmissions.
     * Must be threadsafe
     */    
    private final Map<Long, Reply> lastReplies;

    /* Thread responsible to create and forward batches to leader */
    private final ClientRequestBatcher cBatcher;

//    private NioClientManager nioClientManager;
//    private final int localId;
//    private final int N;

    private final SingleThreadDispatcher dispatcher;
    private final ClientBatchManager batchManager;


    public ClientRequestManager(Replica replica, Paxos paxos, Map<Long, Reply> lastReplies, int executeUB) {
        this.paxos = paxos;
        this.replica = replica;
        this.dispatcher = replica.getReplicaDispatcher();
        this.lastReplies = lastReplies;
//        ProcessDescriptor pd = ProcessDescriptor.getInstance();
//        this.localId = pd.localId;
//        this.N = pd.numReplicas;
        this.batchManager = new ClientBatchManager(paxos, replica);
//        this.currentInstance = executeUB;
        cBatcher = new ClientRequestBatcher(paxos.getNetwork(), batchManager.getBatchStore());
        cBatcher.start();
        
        QueueMonitor.getInstance().registerQueue("pendingCReqs", pendingClientProxies.values());
        
    }


    /**
     * Executes command received directly from specified client.
     * 
     * @param command - received client command
     * @param client - client which request this command
     * @throws InterruptedException 
     * @see ClientCommand
     * @see ClientProxy
     */
    public void onClientRequest(ClientCommand command, NioClientProxy client) throws InterruptedException {
        // Called by a Selector thread.
        assert isInSelectorThread() : "Called by wrong thread: " + Thread.currentThread();

        try {
            switch (command.getCommandType()) {
                case REQUEST:
                    ClientRequest request = command.getRequest();
                    RequestId reqId = request.getRequestId();
                    
                    // It is a new request if - there is no stored reply from the given
                    // client - or the sequence number of the stored request is older.
                    Reply lastReply = lastReplies.get(reqId.getClientId());
                    boolean newRequest = lastReply == null ||
                            reqId.getSeqNumber() > lastReply.getRequestId().getSeqNumber();
                            
                    if (newRequest) {
                        if (logger.isLoggable(Level.FINE)) { 
                            logger.fine("Received: " + request);
                        }

                        // Store the ClientProxy associated with the request. 
                        // Used to send the answer back to the client
                        // Must be stored before proposed, otherwise the reply might be ready
                        // before this thread finishes storing the request.
                        
                        // Flow control. Wait for a permit. May block the selector thread.
                        pendingRequestsSem.acquire();                        
                        pendingClientProxies.put(reqId, client);

                        cBatcher.enqueueRequest(request);
                        
                    } else {
                        
                        // Since the replica only keeps the reply to the last request executed from each client,
                        // it checks if the cached reply is for the given request. If not, there's something
                        // wrong, because the client already received the reply (otherwise it wouldn't send an
                        // a more recent request). I've seen this message on view change. Probably some requests
                        // are not properly discarded.
                        if (lastReply.getRequestId().equals(reqId)) {
                            client.send(new ClientReply(Result.OK, lastReply.toByteArray()));
                        } else {
                            String errorMsg = "Request too old: " + request.getRequestId() +
                                    ", Last reply: " + lastReply.getRequestId();
                            logger.warning(errorMsg);
                            client.send(new ClientReply(Result.NACK, errorMsg.getBytes()));
                        }
                        
                    }
                    break;

                default:
                    logger.warning("Received invalid command " + command + " from " + client);
                    client.send(new ClientReply(Result.NACK, "Unknown command.".getBytes()));
                    break;
            }
        } catch (IOException e) {
            logger.warning("Cannot execute command: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.interrupted();
            return;
        }
    }

//    private void sendCachedReply(NioClientProxy client, ClientRequest request) throws IOException 
//    {
//        Reply lastReply = lastReplies.get(request.getRequestId().getClientId());
//        // Since the replica only keeps the reply to the last request executed from each client,
//        // it checks if the cached reply is for the given request. If not, there's something
//        // wrong, because the client already received the reply (otherwise it wouldn't send an
//        // a more recent request). I've seen this message on view change. Probably some requests
//        // are not properly discarded.
//        if (lastReply.getRequestId().equals(request.getRequestId())) {
//            client.send(new ClientReply(Result.OK, lastReply.toByteArray()));
//        } else {
//            String errorMsg = "Request too old: " + request.getRequestId() +
//                    ", Last reply: " + lastReply.getRequestId();
//            logger.warning(errorMsg);
//            client.send(new ClientReply(Result.NACK, errorMsg.getBytes()));
//        }
//    }

    /* 
     * Called when the replica receives a new request from a local client.
     * Stores the request on the list of pendingRequests, then either forwards
     * it to the leader or, if the replica is the leader, enqueues it in the batcher
     * thread for execution.
     */
//    private void onNewClientRequest(NioClientProxy client, ClientRequest request) throws InterruptedException {
//        // Executed by a selector thread
//        assert isInSelectorThread() : "Not in selector: " + Thread.currentThread().getName();
//        
//        if (logger.isLoggable(Level.FINE)) { 
//            logger.fine("Received: " + request);
//        }
//
//        // Store the ClientProxy associated with the request. 
//        // Used to send the answer back to the client
//        // Must be stored before proposed, otherwise the reply might be ready
//        // before this thread finishes storing the request.
//        pendingClientProxies.put(request.getRequestId(), client);
//        
//        // TODO: Flow control
//        //        // Wait for a permit. May block the selector thread.
//        //        Set<Request> pendingRequests = pendingRequestTL.get();
//        //        // logger.fine("Acquiring permit. " + pendingRequestsSem.availablePermits());
//        //        pendingRequestsSem.acquire();
//
//        cBatcher.enqueueRequest(request);
//    }


    /**
     * Caches the reply from the client. If the connection with the client is
     * still active, then reply is sent.
     * 
     * @param request - request for which reply is generated
     * @param reply - reply to send to client
     */
    public void onRequestExecuted(final ClientRequest request, final Reply reply) {
        assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

        final NioClientProxy client = pendingClientProxies.remove(reply.getRequestId());        
        if (client == null) {
            // Only the replica that received the request has the ClientProxy.
            // The other replicas discard the reply.
            //            if (logger.isLoggable(Level.FINE)) {
            //                logger.fine("Client proxy not found, discarding reply. " + request.getRequestId());
            //            }
        } else {
            SelectorThread sThread = client.getSelectorThread();
            //            logger.fine("Enqueueing reply task on " + sThread.getName());
            // Release the permit while still on the Replica thread. This will release 
            // the selector threads that may be blocked waiting for permits, therefore
            // minimizing the change of deadlock between selector threads waiting for
            // permits that will only be available when a selector thread gets to 
            // execute this task. 
            pendingRequestsSem.release();
            sThread.beginInvoke(new Runnable() {
                @Override
                public void run() {
                    //                    Set<ReplicaRequest> pendingRequests = pendingRequestTL.get();
                    //                    boolean removed = pendingRequests.remove(request);
                    //                    assert removed : "Could not remove request: " + request;
                    //
                    //                    if (logger.isLoggable(Level.FINE)) {
                    //                        logger.fine("Sending reply to client. " + request.getRequestId());
                    //                        logger.fine("pendingRequests.size: " + pendingRequests.size() + ", pendingClientProxies.size: " + pendingClientProxies.size());
                    //                    }
                    try {
                        client.send(new ClientReply(Result.OK, reply.toByteArray()));
                    } catch (IOException e) {
                        // cannot send message to the client;
                        // Client should send request again
                        logger.log(Level.WARNING, "Could not send reply to client. Discarding reply: " +
                                request.getRequestId(), e);
                    }
                }});
        }
    }

    /** 
     * Forwards to the new leader the locally owned requests.
     * @param newView
     */
    public void onViewChange(final int newView) {   
        logger.warning("TODO: forward acks to leader. Current state: " + batchManager.toString());
        //        nioClientManager.executeInAllSelectors(new Runnable() {
        //            @Override
        //            public void run() {
        //                try {
        //                    forwardToNewLeader(newView);
        //                } catch (InterruptedException e) {
        //                    // Set the interrupt flag to force the selector thread to quit.
        //                    Thread.currentThread().interrupt();
        //                }
        //            }});
    }

    /** Called on view change. Send all acks to leader
     * @throws InterruptedException 
     */
    void forwardToNewLeader(int newView) throws InterruptedException {
        assert isInSelectorThread() : "Not a selector thread " + Thread.currentThread();
        logger.info("TODO: forward acks to leader");

        //        // Executed in a selector thread. The pendingRequests set cannot change during this callback
        //        Set<ReplicaRequest> pendingRequests = pendingRequestTL.get();
        //
        //        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        //        int newLeader = pd.getLeaderOfView(newView);        
        //
        //        if (newLeader == pd.localId) {
        //            // If we are the leader, enqueue the requests on the batcher.
        //            if (logger.isLoggable(Level.INFO)) {
        //                logger.info("Enqueing " + pendingRequests.size() + " requests in batcher. " +
        //                        "View/leader: " + newView + "/" + newLeader);
        //                if (logger.isLoggable(Level.FINE)) {
        //                    logger.fine("Requests: " + pendingRequests.toString());
        //                }
        //            }
        //            for (ReplicaRequest request : pendingRequests) {
        //                int curView = paxos.getStorage().getView();
        //                if (newView != curView){
        //                    logger.warning("View changed while enqueuing requests. Aborting " +
        //                            "Previous view/leader: " + newView + "/" + newLeader + 
        //                            ", current: " + curView + "/" + pd.getLeaderOfView(curView));
        //                    return;
        //                }
        //                paxos.enqueueRequest(request);
        //            }
        //
        //        } else {
        //            // We are not the leader.
        //            if (logger.isLoggable(Level.INFO)) {
        //                logger.info("Forwarding " + pendingRequests.size() + " requests to leader. " +
        //                        "View/leader: " + newView + "/" + newLeader);
        //                if (logger.isLoggable(Level.FINE)) {
        //                    logger.fine("Requests: " + pendingRequests.toString());
        //                }
        //            }
        //
        //            // send all requests to the leader. Stop if the view changes.
        //            for (ReplicaRequest request : pendingRequests) {
        //                int curView = paxos.getStorage().getView();
        //                if (newView != curView) {
        //                    logger.warning("View changed while forwarding requests. Aborting. " +
        //                            "Previous view/leader: " + newView + "/" + newLeader + 
        //                            ", current: " + curView + "/" + pd.getLeaderOfView(curView));
        //                    return;
        //                }
        //                // TODO: use the batcher
        //                network.sendMessage(new ForwardClientRequest(request), newLeader);
        //            }
        //        }
    }


 


//    private void tryPropose(ReplicaRequestID rid) throws InterruptedException 
//    {
//        assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();
//
//        RequestInfo rInfo = requests.getRequestInfo(rid);
//        assert rInfo != null;
//        if (rInfo.state != State.NotProposed) {
//            // Propose only if the request is still in the Undecided state.
//            if (logger.isLoggable(Level.FINE)) {
//                logger.fine("Not proposing: " + rInfo);
//            }
//            return;
//        }
//
//        if (paxos.isLeader() && rInfo.isStable()) {
//            if (logger.isLoggable(Level.FINE)) {
//                logger.fine("Enqueuing request: " + rInfo);
//            }
//            // The proposal contains only the rid associated with the request
//            ReplicaRequest request = new ReplicaRequest(rid);        
//            boolean succeeded = paxos.enqueueRequest(request);
//            if (!succeeded) {
//                // TODO: No longer the leader or not accepting requests.
//                logger.warning("Could not enqueue request");
//            } else {
//                rInfo.state = State.Proposed;
//            }
//        }
//    }


    /** 
     * Replica class calls this method when it orders a batch with ReplicaRequestIds. 
     * This method puts enqueues the ids for execution and tries to advance the execution
     * of requests.
     *  
     * @param instance
     * @param batch
     */
    public void onBatchDecided(int instance, Deque<ReplicaRequest> batch) {
        // Called by the protocol thread.
        batchManager.onBatchDecided(instance, batch);
    }

    private boolean isInSelectorThread() {
        return Thread.currentThread() instanceof SelectorThread;
    }


    /**
     * Checks whether we reply for the request with greater or equal request id.
     * 
     * @param newRequest - request from client
     * @return <code>true</code> if we reply to request with greater or equal id
     * @see ReplicaRequest
     */
    private boolean isNewClientRequest(ClientRequest newRequest) {
        Reply lastReply = lastReplies.get(newRequest.getRequestId().getClientId());
        /*
         * It is a new request if - there is no stored reply from the given
         * client - or the sequence number of the stored request is older.
         */
        return lastReply == null ||
                newRequest.getRequestId().getSeqNumber() > lastReply.getRequestId().getSeqNumber();
    }

    static final Logger logger = Logger.getLogger(ClientRequestManager.class.getCanonicalName());
}



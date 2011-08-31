package lsr.paxos.replica;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.ReplicaRequest;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.common.nio.SelectorThread;
import lsr.paxos.Paxos;
import lsr.paxos.messages.AckForwardClientRequest;
import lsr.paxos.messages.ForwardClientRequest;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ReplicaRequests.RequestInfo;
import lsr.paxos.replica.ReplicaRequests.State;
import lsr.paxos.statistics.QueueMonitor;

/**
 * This class handles all commands from the clients. A single instance is used
 * to manage all clients.
 * 
 */
public class RequestManager implements MessageHandler {
    private final static AtomicInteger sequencer = new AtomicInteger(1);

    final Replica replica;
    final Paxos paxos;
    final Network network;

    /** 
     * For each replica, keeps the list of requests received from the replica and
     * their state. 
     */
    private final ReplicaRequests requests;
    /* May contain either an integer or a RequestInfo. 
     * An int i marks the end of the requests decided on batch i
     */ 
    private final Deque executionQueue = new ArrayDeque(256);
    private int currentInstance;

    /*
     * Threading This class is accessed by several threads: 
     * - the SelectorThreads that read the requests from the clients: method execute() 
     * - the Replica thread after executing a request: method handleReply()
     */

    /*
     * The maps pendingClientProxies and lastReplies are accessed by the thread
     * reading requests from clients and by the replica thread. 
     */

    /**
     * Requests received but waiting ordering. request id -> client proxy
     * waiting for the reply. Accessed by Replica and Selector threads.
     */
    private final Map<RequestId, NioClientProxy> pendingClientProxies =
            new ConcurrentHashMap<RequestId, NioClientProxy>();

    /* Each selector thread keeps a private set with the requests it owns. 
     * Sharing a set would result in too much contention. 
     */
    private static final ThreadLocal<Set<ReplicaRequest>> pendingRequestTL = new ThreadLocal<Set<ReplicaRequest>>() {
        protected java.util.Set<ReplicaRequest> initialValue() {
            return new HashSet<ReplicaRequest>(); 
        };  
    };
    /* Limit on the sum of the size of all pendingRequests queues. This is the maximum 
     * number of requests waiting to be ordered and executed this replica will keep before 
     * stopping accepting new requests from clients. When this limit is reached, the 
     * selector threads will block on pendingRequestSem.
     */   
    private static final int MAX_PENDING_REQUESTS = 1024;

    private final Semaphore pendingRequestsSem = new Semaphore(MAX_PENDING_REQUESTS);

    /**
     * Keeps the last reply for each client. Necessary for retransmissions.
     * Must be threadsafe
     */    
    private final Map<Long, Reply> lastReplies;

    /* Thread responsible to create and forward batches to leader */
    private final ForwardThread forwardingThread;

    private NioClientManager nioClientManager;

    private final int localId;

    private final SingleThreadDispatcher dispatcher;

    private final int N;


    public RequestManager(Replica replica, Paxos paxos, Map<Long, Reply> lastReplies, int executeUB) {
        this.paxos = paxos;
        this.replica = replica;
        this.dispatcher = replica.getReplicaDispatcher();
        this.lastReplies = lastReplies;
        this.network = paxos.getNetwork();
        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        this.localId = pd.localId;
        this.N = pd.numReplicas;
        this.requests = new ReplicaRequests();
        this.currentInstance = executeUB;

        Network.addMessageListener(MessageType.ForwardedClientRequest, this);
        Network.addMessageListener(MessageType.AckForwardedRequest, this);


        forwardingThread = new ForwardThread();
        forwardingThread.start();
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
    public void onClientRequest(final ClientCommand command, final NioClientProxy client) throws InterruptedException {
        // Called by a Selector thread.
        assert isInSelectorThread() : "Called by wrong thread: " + Thread.currentThread();

        dispatcher.execute(new Runnable(){
            @Override
            public void run() {
                try {
                    switch (command.getCommandType()) {
                        case REQUEST:
                            ClientRequest request = command.getRequest();

                            if (isNewClientRequest(request)) {
                                onNewClientRequest(client, request);
                            } else {
                                sendCachedReply(client, request);
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
            }});
    }

    private void sendCachedReply(NioClientProxy client, ClientRequest request) throws IOException 
    {
        Reply lastReply = lastReplies.get(request.getRequestId().getClientId());
        // Since the replica only keeps the reply to the last request executed from each client,
        // it checks if the cached reply is for the given request. If not, there's something
        // wrong, because the client already received the reply (otherwise it wouldn't send an
        // a more recent request). I've seen this message on view change. Probably some requests
        // are not properly discarded.
        if (lastReply.getRequestId().equals(request.getRequestId())) {
            client.send(new ClientReply(Result.OK, lastReply.toByteArray()));
        } else {
            String errorMsg = "Request too old: " + request.getRequestId() +
                    ", Last reply: " + lastReply.getRequestId();
            logger.warning(errorMsg);
            client.send(new ClientReply(Result.NACK, errorMsg.getBytes()));
        }
    }

    /* 
     * Called when the replica receives a new request from a local client.
     * Stores the request on the list of pendingRequests, then either forwards
     * it to the leader or, if the replica is the leader, enqueues it in the batcher
     * thread for execution.
     */
    private void onNewClientRequest(NioClientProxy client, ClientRequest request) throws InterruptedException {
        // Executed by a selector thread
        //        assert isInSelectorThread() : "Not in selector: " + Thread.currentThread().getName();
        assert dispatcher.amIInDispatcher() : "Not in dispatcher: " + Thread.currentThread().getName();

        // Store the ClientProxy associated with the request. 
        // Used to send the answer back to the client
        // Must be stored before proposed, otherwise the reply might be ready
        // before this thread finishes storing the request.
        pendingClientProxies.put(request.getRequestId(), client);

        // Request forwarding.
        ReplicaRequestID rid = new ReplicaRequestID(localId, sequencer.getAndIncrement());
        assert !requests.contains(rid) : "Already known. RID: " + rid + ", Request: " + request;
        RequestInfo rInfo = requests.newRequestInfo(rid, request);        
        requests.setRequestInfo(rid, rInfo);
        requests.markReceived(localId, rid);
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Received request from local client: " + rInfo);
        }
        // Send to all
        ForwardClientRequest fReqMsg = new ForwardClientRequest(request, rid, requests.rcvdUB[localId]);
        network.sendToOthers(fReqMsg);

        // TODO: Flow control
        //        // Wait for a permit. May block the selector thread.
        //        Set<Request> pendingRequests = pendingRequestTL.get();
        //        // logger.fine("Acquiring permit. " + pendingRequestsSem.availablePermits());
        //        pendingRequestsSem.acquire();
    }


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

    /* Handler for forwarded requests */
    @Override
    public void onMessageReceived(final Message msg, final int sender) {
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (msg instanceof ForwardClientRequest) {
                        onForwardClientRequest((ForwardClientRequest) msg, sender);
                    } else {
                        onAckForwardClientRequest((AckForwardClientRequest) msg, sender);
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


    /** 
     * Forwards to the new leader the locally owned requests.
     * @param newView
     */
    public void onViewChange(final int newView) {   
        logger.warning("TODO: forward acks to leader. Current state: " + requests.toString());
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

        ClientRequest creq = fReq.request;
        ReplicaRequestID rid = fReq.id;

        RequestInfo rInfo = requests.getRequestInfo(rid);
        // Create a new entry if none exists        
        if (rInfo == null) {
            rInfo = requests.newRequestInfo(rid,creq);
            requests.setRequestInfo(rid, rInfo);
            requests.markReceived(localId, rid);
        }
        if (rInfo.request == null) {
            rInfo.request = creq;            
            requests.markReceived(localId, rid);            
        } else {
            assert rInfo.request.equals(creq): "Old: " + rInfo.request + ", Rcvd: " + creq;  
        }
        requests.markReceived(sender, rid);
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Received request: " + rInfo);
        }        
        int leader = paxos.getLeaderId();
        if (localId != leader) {
            network.sendMessage(new AckForwardClientRequest(rid), leader);
        }
        switch (rInfo.state) {
            case Undecided:
                tryPropose(rid);
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
        requests.markReceived(sender, fReq.rcvdUB);
    }

    public void onAckForwardClientRequest(AckForwardClientRequest ack, int sender) throws InterruptedException 
    {
        assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

        ReplicaRequestID rid = ack.id;
        RequestInfo rInfo = requests.getRequestInfo(rid);
        if (rInfo == null) {
            rInfo = requests.newRequestInfo(rid);
            requests.setRequestInfo(rid, rInfo);
        }
        requests.markReceived(sender, rid);
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Ack from " + sender + " for " + rInfo);
        }
        tryPropose(rid);
    }

    private void tryPropose(ReplicaRequestID rid) throws InterruptedException 
    {
        assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

        RequestInfo rInfo = requests.getRequestInfo(rid);
        assert rInfo != null;
        if (rInfo.state != State.Undecided) {
            // Propose only if the request is still in the Undecided state.
            logger.info("Not proposing: " + rInfo);
            return;
        }

        if (paxos.isLeader() && rInfo.isStable()) {
            logger.info("Proposing request: " + rInfo);
            // The proposal contains only the rid associated with the request
            ReplicaRequest request = new ReplicaRequest(rid);        
            boolean succeeded = paxos.enqueueRequest(request);
            if (!succeeded) {
                // TODO: No longer the leader or not accepting requests.
                logger.info("Could not enqueue request");
            } else {
                rInfo.state = State.Proposed;
            }
        }
    }


    /** 
     * Replica class calls this method when it orders a batch with ReplicaRequestIds. 
     * This method puts enqueues the ids for execution and tries to advance the execution
     * of requests.
     *  
     * @param instance
     * @param batch
     */
    public void onBatchDecided(int instance, Deque<ReplicaRequest> batch) {
        assert dispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

        //        logger.info("Batch decided. Instance: " + instance + ", " + batch.toString());
        for (ReplicaRequest req : batch) {
            ReplicaRequestID rid = req.getRequestId();
            RequestInfo rInfo = requests.getRequestInfo(rid);
            // Decision may be reached before having received the forwarded request
            if (rInfo == null) {
                rInfo = requests.newRequestInfo(req.getRequestId());
                requests.setRequestInfo(rid, rInfo);
            }
            rInfo.state = State.Decided;
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Instance:" + instance + ": " + rInfo.toString());
            }
            executionQueue.add(rInfo);
        }
        // Place a marker to represent the end of the batch for this instance
        executionQueue.add(instance);
        executeRequests();
        
        requests.pruneLogs();
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
                replica.instanceExecuted(instance);
                currentInstance = instance+1;
            } else {
                RequestInfo rInfo = (RequestInfo) obj;
                if (rInfo.request == null) {
                    // Do not yet have the request. Wait.
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Request missing, suspending execution. rid: " + rInfo.rid);
                    }
                    return;
                } else {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Executing. rid: " + rInfo.rid + ". cid: " + rInfo.request.getRequestId());
                    }
                    // execute the request.
                    replica.executeClientRequest(currentInstance, rInfo.request);
                    rInfo.state = State.Executed;
                }
            }
            // The request was handled. Remove it.
            executionQueue.pop();
        }
    }


    private boolean isInSelectorThread() {
        return Thread.currentThread() instanceof SelectorThread;
    }

    private void forwardRequest(ReplicaRequest request) throws InterruptedException {
        // Called by selector thread
        assert isInSelectorThread() : "Not in Selector thread: " + Thread.currentThread().getName();

        int leader = paxos.getLeaderId();
        // This method is called when the request fails to enqueue on the dispatcher. 
        // This happens usually because this replica is not the leader, but can happen
        // during view change, while this replica is preparing a view. In this
        // case, the replica might be the leader, so we should not forward the request.
        if (leader == ProcessDescriptor.getInstance().localId) {
            logger.warning("Not forwarding request: " + request + ". Leader is the local process");
        } else {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Enqueueing request for forwarding " + request.getRequestId());
            }

            // Batching is done by a separate thread.
            forwardingThread.enqueueRequest(request);
        }
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

    void setNioClientManager(NioClientManager nioClientManager) {
        this.nioClientManager = nioClientManager;

        // Monitor queue size
        nioClientManager.executeInAllSelectors(new Runnable() {
            @Override
            public void run() {
                Set<ReplicaRequest> pendingRequests = pendingRequestTL.get();
                QueueMonitor.getInstance().registerQueue(Thread.currentThread().getName() + "-pendingRequestsQueue", pendingRequests);                
            }
        });
    }

    /**
     * This thread builds the batches with the requests received from the client and forwards
     * them to the leader.  The selectors place the requests in a queue managed owned by this
     * class. The ForwardingThread reads requests from this queue and groups them into batches.
     * 
     *  There is some contention between the Selector threads and the Forwarding thread in 
     *  the shared queue, but it should be acceptable. For 4 selectors, in a 180s run:
     * 
     *  <pre>
     *  (# blocked times, blocking time) (#waits, waiting time)
     * Selector-X (67388 3365) (194668  11240) 
     * ForwardingBatcher (95081 3810) (1210222  96749) 
     *  </pre> 
     * 
     * @author Nuno Santos (LSR)
     */
    final class ForwardThread implements Runnable {
        public final static String FORWARD_MAX_BATCH_SIZE = "replica.ForwardMaxBatchSize";
        // Corresponds to a ethernet frame
        public final static int DEFAULT_FORWARD_MAX_BATCH_SIZE = 1450;
        public final int forwardMaxBatchSize;

        // In milliseconds
        public final static String FORWARD_MAX_BATCH_DELAY = "replica.ForwardMaxBatchDelay";
        public final static int DEFAULT_FORWARD_MAX_BATCH_DELAY = 50;
        public final int forwardMaxBatchDelay;

        /* Selector threads enqueue requests in this queue. The Batcher thread takes requests
         * from here to prepare batches.
         */
        private final ArrayBlockingQueue<ReplicaRequest> queue = new ArrayBlockingQueue<ReplicaRequest>(128);

        /* Stores the requests that will make the next batch. We use two queues to minimize 
         * contention between the Selector threads and the Batcher thread, since they only
         * have to contend for the first queue, which is accessed very briefly by either  thread. 
         */
        private final ArrayList<ForwardClientRequest> batch = new ArrayList<ForwardClientRequest>(16);
        // Total size of the requests stored in the batch array.
        private int sizeInBytes = 0;

        private final Thread batcherThread;

        public ForwardThread() {
            ProcessDescriptor pd = ProcessDescriptor.getInstance();        
            this.forwardMaxBatchDelay = pd.config.getIntProperty(FORWARD_MAX_BATCH_DELAY, DEFAULT_FORWARD_MAX_BATCH_DELAY);
            this.forwardMaxBatchSize = pd.config.getIntProperty(FORWARD_MAX_BATCH_SIZE, DEFAULT_FORWARD_MAX_BATCH_SIZE);
            logger.config(FORWARD_MAX_BATCH_DELAY + "=" + forwardMaxBatchDelay);
            logger.config(FORWARD_MAX_BATCH_SIZE + "=" + forwardMaxBatchSize);

            this.batcherThread = new Thread(this, "ForwardBatcher");
        }

        public void start() {
            batcherThread.start();
        }

        public void enqueueRequest(ReplicaRequest req) throws InterruptedException {
            //            logger.fine("Enqueuing request: " + req);
            queue.put(req);
        }

        @Override
        public void run() {
            long batchStart = -1;

            while (true) {
                ReplicaRequest request;
                try {
                    int timeToExpire = (sizeInBytes == 0) ? 
                            Integer.MAX_VALUE :
                                (int) (batchStart+forwardMaxBatchDelay - System.currentTimeMillis());
                    //                    if (logger.isLoggable(Level.FINE)) {
                    //                        logger.fine("Waiting for " + timeToExpire);
                    //                    }
                    request = queue.poll(timeToExpire, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.warning("Thread interrupted. Quitting.");
                    return;
                }

                if (request == null) {
                    //                    logger.fine("Timeout expired.");
                    // Timeout expired
                    sendBatch();
                } else {
                    logger.fine("Request: " + request);
                    // There is a new request to forward
                    //                    ForwardClientRequest fr = new ForwardClientRequest(request);
                    //                    if (sizeInBytes == 0){
                    //                        //                        logger.fine("New batch.");
                    //                        // Batch is empty. Add the new request unconditionally
                    //                        batch.add(fr);
                    //                        sizeInBytes += fr.byteSize();
                    //                        batchStart = System.currentTimeMillis();
                    //                        // A single request might exceed the maximum size. 
                    //                        // If so, send the batch
                    //                        if (sizeInBytes > forwardMaxBatchSize) {
                    //                            sendBatch();
                    //                        }
                    //                    } else {
                    //                        //                        logger.fine("Current batch size: " + sizeInBytes);
                    //                        // Batch is not empty. 
                    //                        if (sizeInBytes + fr.byteSize() > forwardMaxBatchSize) {
                    //                            // Adding this request would exceed the maximum size. 
                    //                            // Send the batch and start a new batch with the current request. 
                    //                            sendBatch();
                    //                            batchStart = System.currentTimeMillis();
                    //                        }
                    //                        batch.add(fr);
                    //                        sizeInBytes += fr.byteSize();
                    //                    }
                }
            }
        }

        private void sendBatch() {
            assert sizeInBytes > 0 : "Trying to send an empty batch.";
            // Forward this batch
            ByteBuffer bb = ByteBuffer.allocate(sizeInBytes);
            for (ForwardClientRequest fReq : batch) {
                fReq.writeTo(bb);
            }
            int leader = paxos.getLeaderId();
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Forwarding batch to leader " + leader + ", size: " + sizeInBytes + ", " + batch);
            }
            //            logger.warning("Batch size: " + sizeInBytes);
            assert bb.remaining() == 0 : "Should be full. Remaining: " + bb.remaining();
            network.send(bb.array(), leader);
            batch.clear();
            sizeInBytes = 0;
        }
    }


    static final Logger logger = Logger.getLogger(RequestManager.class.getCanonicalName());
}



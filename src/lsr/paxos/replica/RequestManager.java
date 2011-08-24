package lsr.paxos.replica;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
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
import lsr.common.PrimitivesByteArray;
import lsr.common.ProcessDescriptor;
import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.common.nio.SelectorThread;
import lsr.paxos.Paxos;
import lsr.paxos.messages.ForwardedRequest;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.statistics.QueueMonitor;

/**
 * This class handles all commands from the clients. A single instance is used
 * to manage all clients.
 * 
 */
public class RequestManager  implements MessageHandler {

    private final Paxos paxos;
    private final Network network;
    /*
     * Threading This class is accessed by several threads: 
     * - the SelectorThreads that read the requests from the clients: method execute() 
     * - the Replica thread after executing a request: method handleReply()
     */

    /*
     * The maps pendingRequests and lastReplies are accessed by the thread
     * reading requests from clients and by the replica thread. 
     */

    /**
     * Requests received but waiting ordering. request id -> client proxy
     * waiting for the reply.
     */
    private final Map<RequestId, NioClientProxy> pendingClientProxies =
            new ConcurrentHashMap<RequestId, NioClientProxy>();

    private static final int MAX_PENDING_REQUESTS = 4*1024;
    // Global limit on the size of all pendingRequests queues.
    private final Semaphore pendingRequestsSem = new Semaphore(MAX_PENDING_REQUESTS);
    private static final ThreadLocal<Set<Request>> pendingRequestTL = new ThreadLocal<Set<Request>>() {
        protected java.util.Set<Request> initialValue() {
            return new HashSet<Request>(); 
        };  
    };

    /**
     * Keeps the last reply for each client. Necessary for retransmissions.
     * Must be threadsafe
     */
    private final Map<Long, Reply> lastReplies;
    private NioClientManager nioClientManager;
    //    private final SingleThreadDispatcher dispatcher;

    public RequestManager(Paxos paxos, Map<Long, Reply> lastReplies) {
        this.paxos = paxos;
        this.lastReplies = lastReplies;
        this.network = paxos.getNetwork();
        Network.addMessageListener(MessageType.ForwardedRequest, this);
    }

    @Override
    public void onMessageReceived(Message msg, int sender) {
        processForwardedRequest(((ForwardedRequest) msg).request);
    }
    @Override
    public void onMessageSent(Message message, BitSet destinations) {
        // Ignore
    }
    /**
     * Executes command received from specified client.
     * 
     * @param command - received client command
     * @param client - client which request this command
     * @throws InterruptedException 
     * @see ClientCommand
     * @see ClientProxy
     */
    public void processNewRequest(ClientCommand command, NioClientProxy client) {
        // Called by a Selector thread.
        try {
            switch (command.getCommandType()) {
                case REQUEST:
                    Request request = command.getRequest();

                    if (isNewRequest(request)) {
                        handleNewRequest(client, request);                        
                    } else {
                        handleOldRequest(client, request);
                    }
                    break;

                default:
                    logger.warning("Received invalid command " + command + " from " + client);
                    client.send(new ClientReply(Result.NACK, "Unknown command.".getBytes()));
                    break;
            }
        } catch (IOException e) {
            logger.warning("Cannot execute command: " + e.getMessage());
        }
    }

    /** 
     * Forwards to the new leader the locally owned requests.
     * @param newView
     */
    public void onViewChange(final int newView) {        
        nioClientManager.executeInAllSelectors(new Runnable() {
            @Override
            public void run() {
                forwardToNewLeader(newView);
            }});
    }

    void forwardToNewLeader(int newView) {
        assert isInSelectorThread() : "Not a selector thread " + Thread.currentThread();
        
        // Executed in a selector thread. The pendingRequests set cannot change during this callback
        Set<Request> pendingRequests = pendingRequestTL.get();
        
        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        int newLeader = pd.getLeaderOfView(newView);        

        if (newLeader == pd.localId) {
            // If we are the leader, enqueue the requests on the batcher.
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Enqueing " + pendingRequests.size() + " requests in batcher. " +
                        "View/leader: " + newView + "/" + newLeader);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Requests: " + pendingRequests.toString());
                }
            }
            for (Request request : pendingRequests) {
                int curView = paxos.getStorage().getView();
                if (newView != curView){
                    logger.warning("View changed while enqueuing requests. Aborting " +
                            "Previous view/leader: " + newView + "/" + newLeader + 
                            ", current: " + curView + "/" + pd.getLeaderOfView(curView));
                    return;
                }
                try {
                    paxos.enqueueRequest(request);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

        } else {
            // We are not the leader.
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Forwarding " + pendingRequests.size() + " requests to leader. " +
                        "View/leader: " + newView + "/" + newLeader);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Requests: " + pendingRequests.toString());
                }
            }

            // send all requests to the leader. Stop if the view changes.
            for (Request request : pendingRequests) {
                int curView = paxos.getStorage().getView();
                if (newView != curView) {
                    logger.warning("View changed while forwarding requests. Aborting. " +
                            "Previous view/leader: " + newView + "/" + newLeader + 
                            ", current: " + curView + "/" + pd.getLeaderOfView(curView));
                    return;
                }           
                network.sendMessage(new ForwardedRequest(request), newLeader);
            }
        }
    }

    /**
     * Caches the reply from the client. If the connection with the client is
     * still active, then reply is sent.
     * 
     * @param request - request for which reply is generated
     * @param reply - reply to send to client
     */
    public void handleReply(final Request request, final Reply reply) {
        // Not needed, already cached in Replica.executeDecided()
        // cache the reply
        //        lastReplies.put(request.getRequestId().getClientId(), reply);

        final NioClientProxy client = pendingClientProxies.remove(reply.getRequestId());        
        if (client == null) {
            // Only the replica that received the request has the ClientProxy.
            // The other replicas discard the reply.
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Client proxy not found, discarding reply. " + request.getRequestId());
            }
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
                    Set<Request> pendingRequests = pendingRequestTL.get();
                    boolean removed = pendingRequests.remove(request);
                    assert removed : "Could not remove request: " + request;

                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Sending reply to client. " + request.getRequestId());
                        logger.fine("pendingRequests.size: " + pendingRequests.size() + ", pendingClientProxies.size: " + pendingClientProxies.size());
                    }
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
     * Handles a request forwarded by another replica. Tries to enqueue it on the
     * current batcher.
     * @param request
     */
    private void processForwardedRequest(Request request) {
        try {
            if (isNewRequest(request)) {
                if (!paxos.enqueueRequest(request)) {
                    logger.warning("Could not enqueue forwarded request: " + request + ". Current leader: " + paxos.getLeaderId() + ". Discarding request.");
                }
            } else {
                logger.warning("Already executed, ignoring forwarded request: " + request);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private boolean isInSelectorThread() {
        return Thread.currentThread() instanceof SelectorThread;
        
    }
    private void handleNewRequest(NioClientProxy client, Request request) throws IOException {
        // Executed by a selector thread
        assert isInSelectorThread() : "Not in selector: " + Thread.currentThread().getName();
        
        try {
            // store for later retrieval by the replica thread (this client
            // proxy will be notified when this request is executed)
            // The request must be stored on the pendingRequests array
            // before being proposed, otherwise the reply might be ready
            // before this thread finishes storing the request.
            // The handleReply method would not know where to send the reply.

            // Wait for a permit
            Set<Request> pendingRequests = pendingRequestTL.get();
//            logger.fine("Acquiring permit. " + pendingRequestsSem.availablePermits());
            pendingRequestsSem.acquire();
            pendingRequests.add(request);

            pendingClientProxies.put(request.getRequestId(), client);
            if (!paxos.enqueueRequest(request)) {
                // Because this method is not called from paxos dispatcher, it is
                // possible that between checking if process is a leader and
                // proposing the request, we lost leadership

                if (ProcessDescriptor.getInstance().forwardClientRequests) {
                    // Forward the request to the leader. Retain responsibility for
                    // the request, keep the ClientProxy to be able to send the answer
                    // when the request is executed.
                    forwardRequest(request);
                } else {
                    redirectToLeader(client);
                    // As we are not going to handle this request, remove the ClientProxy
                    pendingClientProxies.remove(request.getRequestId());
                    pendingRequests.remove(request);
                    pendingRequestsSem.release();
                }
            }
        } catch (InterruptedException e) {
            // paxos.enqueueRequest() may block if the request queue is full. It may
            // be interrupted while blocked, rasing InterruptedException. 
            // As it is not practical to propagate the exception up to the main run()
            // method of this thread, instead set again the interrupt flag 
            Thread.currentThread().interrupt();
        }
    }

    static class BatchInfo {
        int sizeInBytes = 0;
        ArrayList<ForwardedRequest> batch = new ArrayList<ForwardedRequest>(10);
    }

    private static final ThreadLocal<BatchInfo> threadLocalBatch = new ThreadLocal<BatchInfo> () {
        @Override protected BatchInfo initialValue() {
            return new BatchInfo();
        }
    };

    // TODO: Add a timer to the batch, make sure it is sent even if 
    // clients stop sending requests.
    private void forwardRequest(Request request) {
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
                logger.fine("Forwarding request " + request.getRequestId() + " to leader " + leader);
            }
            //            network.sendMessage(new ForwardedRequest(request), leader);

            BatchInfo batchInfo = threadLocalBatch.get();
            ForwardedRequest fr = new ForwardedRequest(request);
            if (batchInfo.sizeInBytes + fr.byteSize() > 1450) {
                // Forward this batch
                ByteBuffer bb = ByteBuffer.allocate(batchInfo.sizeInBytes);
                for (ForwardedRequest fReq : batchInfo.batch) {
                    fReq.writeTo(bb);
                }                
                assert bb.remaining() == 0 : "Should be full. Remaining: " + bb.remaining();
                network.send(bb.array(), leader);
                batchInfo.batch.clear();
                batchInfo.sizeInBytes = 0;
            }                
            batchInfo.batch.add(fr);
            batchInfo.sizeInBytes += fr.byteSize();
        }
    }

    /** 
     * Reply to the client, informing it about who is the current leader.
     * The client is them responsible to connect directly to the leader. 
     *  
     * @param client
     * @throws IOException
     */
    private void redirectToLeader(NioClientProxy client) throws IOException {
        int redirectId = paxos.getLeaderId();
        logger.info("Redirecting client to leader: " + redirectId);
        client.send(new ClientReply(Result.REDIRECT, PrimitivesByteArray.fromInt(redirectId)));
    }

    private void handleOldRequest(NioClientProxy client, Request request) throws IOException {
        Reply lastReply = lastReplies.get(request.getRequestId().getClientId());

        // resent the reply if known
        if (lastReply.getRequestId().equals(request.getRequestId())) {
            client.send(new ClientReply(Result.OK, lastReply.toByteArray()));
        } else {
            String errorMsg = "Request too old: " + request.getRequestId() +
                    ", Last reply: " + lastReply.getRequestId();
            // This happens when the system is under heavy load.
            logger.warning(errorMsg);
            client.send(new ClientReply(Result.NACK, errorMsg.getBytes()));
        }
    }

    /**
     * Checks whether we reply for the request with greater or equal request id.
     * 
     * @param newRequest - request from client
     * @return <code>true</code> if we reply to request with greater or equal id
     * @see Request
     */
    private boolean isNewRequest(Request newRequest) {
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
                Set<Request> pendingRequests = pendingRequestTL.get();
                QueueMonitor.getInstance().registerQueue(Thread.currentThread().getName() + "-pendingRequestsQueue", pendingRequests);                
            }
        });
    }

    private static final Logger logger =
            Logger.getLogger(RequestManager.class.getCanonicalName());
}

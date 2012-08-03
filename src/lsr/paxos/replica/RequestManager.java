package lsr.paxos.replica;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.ClientRequest;
import lsr.common.PrimitivesByteArray;
import lsr.common.ProcessDescriptor;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.nio.SelectorThread;
import lsr.paxos.core.Paxos;
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
public class RequestManager implements MessageHandler {
    public final static String FORWARD_CLIENT_REQUESTS = "replica.ForwardClientRequests";
    public final static boolean DEFAULT_FORWARD_CLIENT_REQUESTS = true;
    public final boolean forwardClientRequests;

    final Paxos paxos;
    final Network network;
    /*
     * Threading This class is accessed by several threads: - the
     * SelectorThreads that read the requests from the clients: method execute()
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

    /*
     * Each selector thread keeps a private set with the requests it owns.
     * Sharing a set would result in too much contention.
     */
    private static final ThreadLocal<Set<ClientRequest>> pendingRequestTL = new ThreadLocal<Set<ClientRequest>>() {
        protected java.util.Set<ClientRequest> initialValue() {
            return new HashSet<ClientRequest>();
        };
    };
    /*
     * Limit on the sum of the size of all pendingRequests queues. This is the
     * maximum number of requests waiting to be ordered and executed this
     * replica will keep before stopping accepting new requests from clients.
     * When this limit is reached, the selector threads will block on
     * pendingRequestSem.
     */
    private static final int MAX_PENDING_REQUESTS = 4096;
    private final Semaphore pendingRequestsSem = new Semaphore(MAX_PENDING_REQUESTS);

    /**
     * Keeps the last reply for each client. Necessary for retransmissions. Must
     * be threadsafe
     */
    private final Map<Long, Reply> lastReplies;

    /* Thread responsible to create and forward batches to leader */
    private final ForwardThread forwardingThread;

    private NioClientManager nioClientManager;

    public RequestManager(Paxos paxos, Map<Long, Reply> lastReplies) {
        this.paxos = paxos;
        this.lastReplies = lastReplies;
        this.network = paxos.getNetwork();

        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        this.forwardClientRequests = pd.config.getBooleanProperty(FORWARD_CLIENT_REQUESTS,
                DEFAULT_FORWARD_CLIENT_REQUESTS);
        logger.config("Forwarding client requests: " + forwardClientRequests);

        if (forwardClientRequests) {
            Network.addMessageListener(MessageType.ForwardedRequest, this);
            forwardingThread = new ForwardThread();
            forwardingThread.start();
        } else {
            forwardingThread = null;
        }
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
    public void processClientRequest(ClientCommand command, NioClientProxy client)
            throws InterruptedException {
        // Called by a Selector thread.
        assert isInSelectorThread() : "Called by wrong thread: " + Thread.currentThread();

        try {
            switch (command.getCommandType()) {
                case REQUEST:
                    ClientRequest request = command.getRequest();

                    if (isNewRequest(request)) {
                        handleNewRequest(client, request);
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
        }
    }

    /*
     * Called when the replica receives a new request from a local client.
     * Stores the request on the list of pendingRequests, then either forwards
     * it to the leader or, if the replica is the leader, enqueues it in the
     * batcher thread for execution.
     */
    private void handleNewRequest(NioClientProxy client, ClientRequest request)
            throws InterruptedException {
        // Executed by a selector thread
        assert isInSelectorThread() : "Not in selector: " + Thread.currentThread().getName();

        // store for later retrieval by the replica thread (this client
        // proxy will be notified when this request is executed)
        // The request must be stored on the pendingRequests array
        // before being proposed, otherwise the reply might be ready
        // before this thread finishes storing the request.
        // The handleReply method would not know where to send the reply.

        // Wait for a permit. May block the selector thread.
        Set<ClientRequest> pendingRequests = pendingRequestTL.get();
        // logger.fine("Acquiring permit. " +
        // pendingRequestsSem.availablePermits());
        pendingRequestsSem.acquire();
        pendingRequests.add(request);

        pendingClientProxies.put(request.getRequestId(), client);
        // may block if the request queue is full. It may be interrupted while
        // blocked,
        // raising InterruptedException.
        if (!paxos.enqueueRequest(request)) {
            // Because this method is not called from paxos dispatcher, it is
            // possible that between checking if process is a leader and
            // proposing the request, we lost leadership

            if (forwardClientRequests) {
                // Forward the request to the leader. Retain responsibility for
                // the request, keep the ClientProxy to be able to send the
                // answer
                // when the request is executed.
                forwardRequest(request);
            } else {
                redirectToLeader(client);
                // As we are not going to handle this request, remove the
                // ClientProxy
                pendingClientProxies.remove(request.getRequestId());
                pendingRequests.remove(request);
                pendingRequestsSem.release();
            }
        }

    }

    private void sendCachedReply(NioClientProxy client, ClientRequest request) throws IOException {
        Reply lastReply = lastReplies.get(request.getRequestId().getClientId());
        // Since the replica only keeps the reply to the last request executed
        // from each client,
        // it checks if the cached reply is for the given request. If not,
        // there's something
        // wrong, because the client already received the reply (otherwise it
        // wouldn't send an
        // a more recent request). I've seen this message on view change.
        // Probably some requests
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

    /**
     * Caches the reply from the client. If the connection with the client is
     * still active, then reply is sent.
     * 
     * @param request - request for which reply is generated
     * @param reply - reply to send to client
     */
    public void handleReply(final ClientRequest request, final Reply reply) {
        // TODO: JK reply is currently added to lastReplies in Replica class
        final NioClientProxy client = pendingClientProxies.remove(reply.getRequestId());
        if (client == null) {
            // Only the replica that received the request has the ClientProxy.
            // The other replicas discard the reply.
        } else {
            SelectorThread sThread = client.getSelectorThread();
            /*
             * Release the permit while still on the Replica thread. This will
             * release the selector threads that may be blocked waiting for
             * permits, therefore minimizing the chance of deadlock between
             * selector threads waiting for permits that will only be available
             * when a selector thread gets to execute this task.
             */

            pendingRequestsSem.release();
            sThread.beginInvoke(new Runnable() {
                @Override
                public void run() {
                    Set<ClientRequest> pendingRequests = pendingRequestTL.get();
                    boolean removed = pendingRequests.remove(request);
                    // assert removed : "Could not remove request: " + request;
                    if (!removed) {
                        logger.warning("Could not remove request: " + request);
                    }

                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Sending reply to client. " + request.getRequestId());
                        logger.fine("pendingRequests.size: " + pendingRequests.size() +
                                    ", pendingClientProxies.size: " + pendingClientProxies.size());
                    }
                    try {
                        client.send(new ClientReply(Result.OK, reply.toByteArray()));
                    } catch (IOException e) {
                        // cannot send message to the client;
                        // Client should send request again
                        logger.log(Level.WARNING,
                                "Could not send reply to client. Discarding reply: " +
                                        request.getRequestId(), e);
                    }
                }
            });
        }
    }

    /* Handler for forwarded requests */
    @Override
    public void onMessageReceived(Message msg, int sender) {
        try {
            processForwardedRequest(((ForwardedRequest) msg).request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onMessageSent(Message message, BitSet destinations) {
        // Ignore
    }

    /**
     * Forwards to the new leader the locally owned requests.
     * 
     * @param newView
     */
    public void onViewChange(final int newView) {
        if (forwardClientRequests) {
            nioClientManager.executeInAllSelectors(new Runnable() {
                @Override
                public void run() {
                    try {
                        forwardToNewLeader(newView);
                    } catch (InterruptedException e) {
                        // Set the interrupt flag to force the selector thread
                        // to quit.
                        Thread.currentThread().interrupt();
                    }
                }
            });
        } else {
            sendRedirects(newView);
        }
    }

    private void sendRedirects(final int newView) {
        final int newLeader = ProcessDescriptor.getInstance().getLeaderOfView(newView);
        if (logger.isLoggable(Level.WARNING))
            logger.warning("Redirecting all clients to replica " + newLeader);

        // Start by creating a list of client proxies for each Selector thread.
        // This reduces the number of tasks that have to be scheduled, from one
        // per
        // client proxy to one per selector thread.
        // In the process, release all the permits from the pending requests
        // semaphore and
        // clear the set of pendingClientProxies.

        // SelectorThread is used as key, using identity for equality and
        // hashcode.
        // This is the correct identity for these objects, as an instance is
        // only equal to itself.
        int permits = 0;
        Map<SelectorThread, List<NioClientProxy>> proxies = new HashMap<SelectorThread, List<NioClientProxy>>();
        for (final NioClientProxy client : pendingClientProxies.values()) {
            SelectorThread sThread = client.getSelectorThread();
            List<NioClientProxy> proxyList = proxies.get(sThread);
            if (proxyList == null) {
                proxyList = new ArrayList<NioClientProxy>();
                proxies.put(sThread, proxyList);
            }
            proxyList.add(client);
            permits++;
        }
        pendingRequestsSem.release(permits);
        pendingClientProxies.clear();

        for (Entry<SelectorThread, List<NioClientProxy>> v : proxies.entrySet()) {
            SelectorThread sThread = v.getKey();
            final List<NioClientProxy> list = v.getValue();

            sThread.beginInvoke(new Runnable() {
                @Override
                public void run() {
                    Set<ClientRequest> pendingRequests = pendingRequestTL.get();
                    pendingRequests.clear();

                    for (NioClientProxy client : list) {
                        try {
                            client.send(new ClientReply(Result.REDIRECT,
                                    PrimitivesByteArray.fromInt(newLeader)));
                            client.closeConnection();
                        } catch (IOException e) {
                            // cannot send message to the client;
                            // Client should send request again
                            logger.log(Level.WARNING, "Could not send redirect to client.", e);
                        }
                    }
                }
            });
        }
    }

    /**
     * Called on view change. Every selector will send the requests it owns to
     * the new leader.
     * 
     * @throws InterruptedException
     */
    void forwardToNewLeader(int newView) throws InterruptedException {
        assert forwardClientRequests : "Should not be called. Forwarding client request disabled.";
        assert isInSelectorThread() : "Not a selector thread " + Thread.currentThread();

        // Executed in a selector thread. The pendingRequests set cannot change
        // during this callback
        Set<ClientRequest> pendingRequests = pendingRequestTL.get();

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
            for (ClientRequest request : pendingRequests) {
                int curView = paxos.getStorage().getView();
                if (newView != curView) {
                    logger.warning("View changed while enqueuing requests. Aborting " +
                                   "Previous view/leader: " + newView + "/" + newLeader +
                                   ", current: " + curView + "/" + pd.getLeaderOfView(curView));
                    return;
                }
                paxos.enqueueRequest(request);
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
            for (ClientRequest request : pendingRequests) {
                int curView = paxos.getStorage().getView();
                if (newView != curView) {
                    logger.warning("View changed while forwarding requests. Aborting. " +
                                   "Previous view/leader: " + newView + "/" + newLeader +
                                   ", current: " + curView + "/" + pd.getLeaderOfView(curView));
                    return;
                }
                // TODO: NS: use the batcher
                network.sendMessage(new ForwardedRequest(request), newLeader);
            }
        }
    }

    /**
     * Handles a request forwarded by another replica. Tries to enqueue it on
     * the current batcher.
     * 
     * @param request
     * @throws InterruptedException
     */
    private void processForwardedRequest(ClientRequest request) throws InterruptedException {
        assert forwardClientRequests : "Should not be called. Forwarding client request disabled.";
        if (isNewRequest(request)) {
            if (!paxos.enqueueRequest(request)) {
                logger.warning("Could not enqueue forwarded request: " + request +
                               ". Current leader: " + paxos.getLeaderId() + ". Discarding request.");
            }
        } else {
            logger.warning("Already executed, ignoring forwarded request: " + request);
        }

    }

    private boolean isInSelectorThread() {
        return Thread.currentThread() instanceof SelectorThread;
    }

    private void forwardRequest(ClientRequest request) throws InterruptedException {
        // Called by selector thread
        assert forwardClientRequests : "Should not be called. Forwarding client request disabled.";
        assert isInSelectorThread() : "Not in Selector thread: " + Thread.currentThread().getName();

        int leader = paxos.getLeaderId();
        // This method is called when the request fails to enqueue on the
        // dispatcher.
        // This happens usually because this replica is not the leader, but can
        // happen
        // during view change, while this replica is preparing a view. In this
        // case, the replica might be the leader, so we should not forward the
        // request.
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
     * Reply to the client, informing it about who is the current leader. The
     * client is them responsible to connect directly to the leader.
     * 
     * @param client
     * @throws IOException
     */
    private void redirectToLeader(NioClientProxy client) {
        int redirectId = paxos.getLeaderId();
        logger.info("Redirecting client to leader: " + redirectId);
        try {
            client.send(new ClientReply(Result.REDIRECT, PrimitivesByteArray.fromInt(redirectId)));
        } catch (IOException e) {
            logger.warning("Error sending reply to client: " + e.getMessage());
            client.closeConnection();
        }
    }

    /**
     * Checks whether we reply for the request with greater or equal request id.
     * 
     * @param newRequest - request from client
     * @return <code>true</code> if we reply to request with greater or equal id
     * @see ClientRequest
     */
    private boolean isNewRequest(ClientRequest newRequest) {
        Reply lastReply = lastReplies.get(newRequest.getRequestId().getClientId());
        /*
         * It is a new request if - there is no stored reply from the given
         * client - or the sequence number of the stored request is older.
         */
        return lastReply == null ||
                newRequest.getRequestId().getSeqNumber() > lastReply.getRequestId().getSeqNumber();
    }

    public void setNioClientManager(NioClientManager nioClientManager) {
        this.nioClientManager = nioClientManager;

        // Monitor queue size
        nioClientManager.executeInAllSelectors(new Runnable() {
            @Override
            public void run() {
                Set<ClientRequest> pendingRequests = pendingRequestTL.get();
                QueueMonitor.getInstance().registerQueue(
                        Thread.currentThread().getName() + "-pendingRequestsQueue", pendingRequests);
            }
        });
    }

    /**
     * This thread builds the batches with the requests received from the client
     * and forwards them to the leader. The selectors place the requests in a
     * queue managed owned by this class. The ForwardingThread reads requests
     * from this queue and groups them into batches.
     * 
     * There is some contention between the Selector threads and the Forwarding
     * thread in the shared queue, but it should be acceptable. For 4 selectors,
     * in a 180s run:
     * 
     * <pre>
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

        /*
         * Selector threads enqueue requests in this queue. The Batcher thread
         * takes requests from here to prepare batches.
         */
        private final ArrayBlockingQueue<ClientRequest> queue = new ArrayBlockingQueue<ClientRequest>(
                128);

        /*
         * Stores the requests that will make the next batch. We use two queues
         * to minimize contention between the Selector threads and the Batcher
         * thread, since they only have to contend for the first queue, which is
         * accessed very briefly by either thread.
         */
        private final ArrayList<ForwardedRequest> batch = new ArrayList<ForwardedRequest>(16);
        // Total size of the requests stored in the batch array.
        private int sizeInBytes = 0;

        private final Thread batcherThread;

        public ForwardThread() {
            ProcessDescriptor pd = ProcessDescriptor.getInstance();
            this.forwardMaxBatchDelay = pd.config.getIntProperty(FORWARD_MAX_BATCH_DELAY,
                    DEFAULT_FORWARD_MAX_BATCH_DELAY);
            this.forwardMaxBatchSize = pd.config.getIntProperty(FORWARD_MAX_BATCH_SIZE,
                    DEFAULT_FORWARD_MAX_BATCH_SIZE);
            logger.config(FORWARD_MAX_BATCH_DELAY + "=" + forwardMaxBatchDelay);
            logger.config(FORWARD_MAX_BATCH_SIZE + "=" + forwardMaxBatchSize);

            this.batcherThread = new Thread(this, "ForwardBatcher");
        }

        public void start() {
            batcherThread.start();
        }

        public void enqueueRequest(ClientRequest req) throws InterruptedException {
            queue.put(req);
        }

        @Override
        public void run() {
            long batchStart = -1;

            while (true) {
                ClientRequest request;
                try {
                    int timeToExpire = (sizeInBytes == 0)
                            ?
                            Integer.MAX_VALUE
                            :
                                (int) (batchStart + forwardMaxBatchDelay - System.currentTimeMillis());
                    request = queue.poll(timeToExpire, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.warning("Thread interrupted. Quitting.");
                    return;
                }

                if (request == null) {
                    // Timeout expired
                    sendBatch();
                } else {
                    logger.fine("Request: " + request);
                    // There is a new request to forward
                    ForwardedRequest fr = new ForwardedRequest(request);
                    if (sizeInBytes == 0) {
                        // Batch is empty. Add the new request unconditionally
                        batch.add(fr);
                        sizeInBytes += fr.byteSize();
                        batchStart = System.currentTimeMillis();
                        // A single request might exceed the maximum size.
                        // If so, send the batch
                        if (sizeInBytes >= forwardMaxBatchSize) {
                            sendBatch();
                        }
                    } else {
                        // Batch is not empty.
                        if (sizeInBytes + fr.byteSize() > forwardMaxBatchSize) {
                            // Adding this request would exceed the maximum
                            // size.
                            // Send the batch and start a new batch with the
                            // current request.
                            sendBatch();
                            batchStart = System.currentTimeMillis();
                        }
                        batch.add(fr);
                        sizeInBytes += fr.byteSize();
                        if (sizeInBytes == forwardMaxBatchSize) {
                            sendBatch();
                            batchStart = System.currentTimeMillis();
                        }
                    }
                }
            }
        }

        private void sendBatch() {
            assert sizeInBytes > 0 : "Trying to send an empty batch.";
            // Forward this batch
            ByteBuffer bb = ByteBuffer.allocate(sizeInBytes);
            for (ForwardedRequest fReq : batch) {
                fReq.writeTo(bb);
            }
            int leader = paxos.getLeaderId();
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Forwarding batch to leader " + leader + ", size: " + sizeInBytes +
                            ", " + batch);
            }
            assert bb.remaining() == 0 : "Should be full. Remaining: " + bb.remaining();
            if (ProcessDescriptor.processDescriptor.localId != leader) {
                network.send(bb.array(), leader);
            } else {
                // ::TODO:: FIXME!!! ::TODO::
                // FIXME!!! ::TODO:: FIXME!!!
                // ::TODO:: FIXME!!! ::TODO::
                // during leader change this optimization slows down whole
                // system a very lot. Fix.
            }
            batch.clear();
            sizeInBytes = 0;
        }
    }

    static final Logger logger = Logger.getLogger(RequestManager.class.getCanonicalName());
}
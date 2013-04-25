package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.ClientRequest;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.common.nio.SelectorThread;
import lsr.paxos.core.Paxos;

/**
 * Handles all commands from the clients. A single instance is used to manage
 * all clients.
 */
final public class ClientRequestManager {

    /*
     * Threading This class is accessed by several threads:
     * 
     * - the SelectorThreads that read the requests from the clients: method
     * execute()
     * 
     * - the Replica thread after executing a request: method handleReply()
     * 
     * The maps pendingClientProxies and lastReplies are accessed by the thread
     * reading requests from clients and by the replica thread.
     */

    /*
     * Flow control: bound on the number of requests that can be in the system,
     * waiting to be ordered and executed. When this limit is reached, the
     * selector threads will block on pendingRequestSem.
     */
    private static final int MAX_PENDING_REQUESTS = 1 * 1024;
    private final Semaphore pendingRequestsSem = new Semaphore(MAX_PENDING_REQUESTS);
    private static final boolean USE_FLOW_CONTROL = true;

    /**
     * Requests received but waiting ordering. request id -> client proxy
     * waiting for the reply. Accessed by Replica and Selector threads.
     */
    private final Map<RequestId, ClientProxy> pendingClientProxies =
            new ConcurrentHashMap<RequestId, ClientProxy>((int) (MAX_PENDING_REQUESTS * 1.5),
                    (float) 0.75, 8);

    private final static ClientProxy NULL_CLIENT_PROXY = new ClientProxy() {
        public void send(ClientReply clientReply) {
        }
    };

    /**
     * Keeps the last reply for each client. Necessary for retransmissions. Must
     * be threadsafe
     */
    private final Map<Long, Reply> lastReplies;

    /* Thread responsible to create and forward batches to leader */
    private final ClientRequestBatcher cBatcher;

    private final SingleThreadDispatcher replicaDispatcher;
    private final ClientBatchManager batchManager;
    private final Paxos paxos;
    private NioClientManager clientManager = null;

    public ClientRequestManager(Replica replica, DecideCallback decideCallback,
                                Map<Long, Reply> lastReplies,
                                ClientBatchManager batchManager, Paxos paxos) {
        assert processDescriptor.indirectConsensus;
        replicaDispatcher = replica.getReplicaDispatcher();
        this.lastReplies = lastReplies;
        this.batchManager = batchManager;
        this.paxos = paxos;
        cBatcher = new ClientRequestBatcher(batchManager, decideCallback);
        cBatcher.start();
    }

    public ClientRequestManager(Replica replica, DecideCallbackImpl decideCallback,
                                Map<Long, Reply> lastReplies,
                                ClientRequestForwarder requestForwarder, Paxos paxos) {
        assert !processDescriptor.indirectConsensus;
        replicaDispatcher = replica.getReplicaDispatcher();
        this.lastReplies = lastReplies;
        this.paxos = paxos;
        batchManager = null;
        cBatcher = new ClientRequestBatcher(requestForwarder, decideCallback);
        cBatcher.start();
    }

    public void setClientManager(NioClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public void dispatchOnClientRequest(final ClientCommand cc, final ClientProxy icp) {
        if (clientManager == null)
            return;
        clientManager.getNextThread().beginInvoke(new Runnable() {

            @Override
            public void run() {
                try {
                    onClientRequest(cc, icp);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

    }

    public void dispatchOnClientRequest(final ClientRequest[] crs, final ClientProxy icp) {
        if (clientManager == null)
            return;
        clientManager.getNextThread().beginInvoke(new Runnable() {

            @Override
            public void run() {
                try {
                    for (ClientRequest cr : crs)
                        onClientRequest(cr, icp);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

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
    public void onClientRequest(ClientCommand command, ClientProxy client)
            throws InterruptedException {

        assert isInSelectorThread() : "Called by wrong thread: " + Thread.currentThread();

        switch (command.getCommandType()) {
            case REQUEST:
                ClientRequest request = command.getRequest();
                onClientRequest(request, client);
                break;

            default:
                logger.warning("Received invalid command " + command + " from " + client);
                client.send(new ClientReply(Result.NACK, "Unknown command.".getBytes()));
                break;
        }
    }

    private void onClientRequest(ClientRequest request, ClientProxy client)
            throws InterruptedException {
        RequestId reqId = request.getRequestId();

        /*
         * It is a new request if
         * 
         * - there is no stored reply from the given client
         * 
         * - or the sequence number of the stored request is older.
         */
        Reply lastReply = lastReplies.get(reqId.getClientId());
        boolean newRequest = lastReply == null ||
                             reqId.getSeqNumber() > lastReply.getRequestId().getSeqNumber();

        if (newRequest) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Received client request: " + request);
            }

            /*
             * Flow control. Wait for a permit. May block the selector thread.
             */

            if (USE_FLOW_CONTROL)
                if (!pendingClientProxies.containsKey(reqId))
                    pendingRequestsSem.acquire();

            /*
             * Store the ClientProxy associated with the request. Used to send
             * the answer back to the client
             */
            if (client != null)
                pendingClientProxies.put(reqId, client);
            else if (USE_FLOW_CONTROL)
                pendingClientProxies.put(reqId, NULL_CLIENT_PROXY);

            if (!processDescriptor.indirectConsensus && paxos.isLeader()) {
                paxos.enqueueRequest(request);
            } else {
                cBatcher.enqueueRequest(request);
            }
        } else {
            if (client == null)
                return;
            /*
             * Since the replica only keeps the reply to the last request
             * executed from each client, it checks if the cached reply is for
             * the given request. If not, there's something wrong, because the
             * client already received the reply (otherwise it wouldn't send an
             * a more recent request).
             */
            if (lastReply.getRequestId().equals(reqId)) {
                client.send(new ClientReply(Result.OK, lastReply.toByteArray()));
            } else {
                String errorMsg = "Request too old: " + request.getRequestId() +
                                  ", Last reply: " + lastReply.getRequestId();
                logger.warning(errorMsg);
                client.send(new ClientReply(Result.NACK, errorMsg.getBytes()));
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
    public void onRequestExecuted(final ClientRequest request, final Reply reply) {
        assert replicaDispatcher.amIInDispatcher() : "Not in replica dispatcher. " +
                                                     Thread.currentThread().getName();

        final ClientProxy client = pendingClientProxies.remove(reply.getRequestId());
        if (client == null) {
            // Only the replica that received the request has the ClientProxy.
            // The other replicas discard the reply.
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("Client proxy not found, discarding reply. " +
                              request.getRequestId());
            }
        } else if (USE_FLOW_CONTROL && client == NULL_CLIENT_PROXY) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("Forwarded request, discarding reply" +
                              request.getRequestId());
            }

            if (USE_FLOW_CONTROL)
                pendingRequestsSem.release();
        } else {
            if (logger.isLoggable(Level.FINEST))
                logger.finest("Enqueueing reply " + reply.getRequestId());
            /*
             * Release the permit while still on the Replica thread. This will
             * release the selector threads that may be blocked waiting for
             * permits, therefore minimizing the change of deadlock between
             * selector threads waiting for permits that will only be available
             * when a selector thread gets to execute this task.
             */
            if (USE_FLOW_CONTROL)
                pendingRequestsSem.release();

            ClientReply clientReply = new ClientReply(Result.OK, reply.toByteArray());
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Scheduling sending reply: " + request.getRequestId() + " " +
                            clientReply);
            }

            client.send(clientReply);
        }
    }

    private boolean isInSelectorThread() {
        return Thread.currentThread() instanceof SelectorThread;
    }

    public ClientBatchManager getClientBatchManager() {
        return batchManager;
    }

    static final Logger logger = Logger.getLogger(ClientRequestManager.class.getCanonicalName());
}

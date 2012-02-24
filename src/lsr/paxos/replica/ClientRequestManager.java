package lsr.paxos.replica;

import java.io.IOException;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientBatch;
import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.ClientRequest;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.common.nio.SelectorThread;
import lsr.paxos.Paxos;
import lsr.paxos.statistics.QueueMonitor;

/**
 * Handles all commands from the clients. A single instance is used to manage all clients.
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
    private static final int MAX_PENDING_REQUESTS = 1*1024;
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

    private final SingleThreadDispatcher replicaDispatcher;
    private final ClientBatchManager batchManager;


    public ClientRequestManager(Replica replica, Paxos paxos, Map<Long, Reply> lastReplies, int executeUB) {
        this.paxos = paxos;
        this.replica = replica;
        this.replicaDispatcher = replica.getReplicaDispatcher();
        this.lastReplies = lastReplies;
        this.batchManager = new ClientBatchManager(paxos, replica);
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

    /**
     * Caches the reply from the client. If the connection with the client is
     * still active, then reply is sent.
     * 
     * @param request - request for which reply is generated
     * @param reply - reply to send to client
     */
    public void onRequestExecuted(final ClientRequest request, final Reply reply) {
        assert replicaDispatcher.amIInDispatcher() : "Not in replica dispatcher. " + Thread.currentThread().getName();

        final NioClientProxy client = pendingClientProxies.remove(reply.getRequestId());        
        if (client == null) {
            // Only the replica that received the request has the ClientProxy.
            // The other replicas discard the reply.
            //            if (logger.isLoggable(Level.FINE)) {
            //                logger.fine("Client proxy not found, discarding reply. " + request.getRequestId());
            //            }
        } else {
//            if (logger.isLoggable(Level.FINE)) 
            logger.fine("Enqueueing reply " + reply.getRequestId());
            // Release the permit while still on the Replica thread. This will release 
            // the selector threads that may be blocked waiting for permits, therefore
            // minimizing the change of deadlock between selector threads waiting for
            // permits that will only be available when a selector thread gets to 
            // execute this task. 
            SelectorThread sThread = client.getSelectorThread();
            pendingRequestsSem.release();
            sThread.beginInvoke(new Runnable() {
                @Override
                public void run() {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Sending reply. " + request.getRequestId());
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
     * Replica class calls this method when it orders a batch with ReplicaRequestIds. 
     * This method puts enqueues the ids for execution and tries to advance the execution
     * of requests.
     *  
     * @param instance
     * @param batch
     */
    public void onBatchDecided(int instance, Deque<ClientBatch> batch) {
        // Called by the protocol thread.
        batchManager.onBatchDecided(instance, batch);
    }

    private boolean isInSelectorThread() {
        return Thread.currentThread() instanceof SelectorThread;
    }

    public ClientBatchManager getClientBatchManager() {
        return batchManager;
    }
    
    static final Logger logger = Logger.getLogger(ClientRequestManager.class.getCanonicalName());
}

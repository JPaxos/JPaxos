package lsr.paxos.replica;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.PrimitivesByteArray;
import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.paxos.NotLeaderException;
import lsr.paxos.Paxos;

/**
 * This class handles all commands from the clients. A single instance is used
 * to manage all clients.
 * 
 */
public class ReplicaCommandCallback implements CommandCallback {
    private final Paxos paxos;
    /*
     * Threading This class is accessed by two threads: - the SelectorThread
     * that reads the requests from the clients: method execute() - the Replica
     * thread after executing a request: method handleReply()
     */

    /*
     * The maps pendingRequests and lastReplies are accessed by the thread
     * reading requests from clients and by the replica thread. The default
     * concurrency factor, 16, is too high. 2 should be enough.
     */

    /**
     * Requests received but waiting ordering. request id -> client proxy
     * waiting for the reply.
     */
    private final ConcurrentHashMap<RequestId, ClientProxy> pendingRequests =
            new ConcurrentHashMap<RequestId, ClientProxy>(32, 2);

    /**
     * Keeps the last reply for each client. Necessary for retransmissions.
     */
    private final ConcurrentHashMap<Long, Reply> lastReplies;

    public ReplicaCommandCallback(Paxos paxos, ConcurrentHashMap<Long, Reply> lastReplies) {
        this.paxos = paxos;
        this.lastReplies = lastReplies;
    }

    /**
     * Executes command received from specified client.
     * 
     * @param command - received client command
     * @param client - client which request this command
     * @see ClientCommand
     * @see ClientProxy
     */
    public void execute(ClientCommand command, ClientProxy client) {
        try {
            switch (command.getCommandType()) {
                case REQUEST:
                    if (!Replica.BENCHMARK) {
                        if (logger.isLoggable(Level.INFO)) {
                            logger.info("Received request " + command.getRequest());
                        }
                    }
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
     * Caches the reply from the client. If the connection with the client is
     * still active, then reply is sent.
     * 
     * @param request - request for which reply is generated
     * @param reply - reply to send to client
     */
    public void handleReply(Request request, Reply reply) {
        // cache the reply
        lastReplies.put(request.getRequestId().getClientId(), reply);

        ClientProxy client = pendingRequests.remove(reply.getRequestId());
        if (client == null) {
            if (paxos.isLeader()) {
                // Only the primary has the ClientProxy.
                // The other replicas discard the reply.
                logger.warning("Client proxy not found, discarding reply. " +
                                request.getRequestId());
            }
            return;
        }

        try {
            client.send(new ClientReply(Result.OK, reply.toByteArray()));
        } catch (IOException e) {
            // cannot send message to the client;
            // Client should send request again
            logger.log(Level.WARNING, "Could not send reply to client. Discarding reply: " +
                                       request.getRequestId(), e);
        }
    }

    private void handleNewRequest(ClientProxy client, Request request) throws IOException {
        // called by the IO threads
        if (!paxos.isLeader()) {
            redirectToLeader(client);
            return;
        }

        try {
            // store for later retrieval by the replica thread (this client
            // proxy will be notified when this request will
            // be executed)
            // The request must be stored on the pendingRequests array
            // before being proposed, otherwise the reply might be ready
            // before this thread finishes storing the request.
            // The handleReply method would not know where to send the reply.
            pendingRequests.put(request.getRequestId(), client);
            paxos.propose(request);
        } catch (NotLeaderException e) {
            // Because this method is not called from paxos dispatcher, it is
            // possible that between checking if process is a leader and
            // proposing the request, we lost leadership
            redirectToLeader(client);
        }
    }

    private void redirectToLeader(ClientProxy client) throws IOException {
        int redirectId = paxos.getLeaderId();
        logger.info("Redirecting client to leader: " + redirectId);
        client.send(new ClientReply(Result.REDIRECT, PrimitivesByteArray.fromInt(redirectId)));
    }

    private void handleOldRequest(ClientProxy client, Request request) throws IOException {
        Reply lastReply = lastReplies.get(request.getRequestId().getClientId());

        // resent the reply if known
        if (lastReply.getRequestId().equals(request.getRequestId())) {
            client.send(new ClientReply(Result.OK, lastReply.toByteArray()));
        } else {
            String errorMsg = "Request too old. " + "Request: " + request.getRequestId() +
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

    private static final Logger logger =
            Logger.getLogger(ReplicaCommandCallback.class.getCanonicalName());
}

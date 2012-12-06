package lsr.paxos.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientCommand.CommandType;
import lsr.common.ClientReply;
import lsr.common.ClientRequest;
import lsr.common.Configuration;
import lsr.common.MovingAverage;
import lsr.common.PID;
import lsr.common.Reply;
import lsr.common.RequestId;

/**
 * Class represents TCP connection to replica. It should be used by clients, to
 * communicates with service on replicas. Only one request can be sent by client
 * at the same time. After receiving reply, next request can be sent.
 * 
 * <p>
 * Example of usage:
 * <p>
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws IOException {
 *  Client client = new Client();
 *  client.connect();
 *  byte[] request = new byte[] { 0, 1, 2 };
 *  byte[] reply = client.execute(request);
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * If use case is that replica & client are located on the same machine and fail
 * as one, one can use a constructor taking as parameter the replica ID that the
 * client is bound to.
 */
public class Client {

    /*
     * Minimum time to wait before reconnecting after a connection failure
     * (connection reset or refused).
     */
    private static final int CONNECTION_FAILURE_TIMEOUT = 500;

    /*
     * Minimum time to wait before reconnecting to a new replica after receiving
     * a redirect
     */
    private static final int REDIRECT_TIMEOUT = 100;

    /*
     * How long to wait for an answer from the replica before connecting to
     * another replica.
     */
    private static final int SOCKET_TIMEOUT = 3000;

    // Connection timeout management - exponential moving average with upper
    // bound on max timeout. Timeout == TO_MULTIPLIER*average
    private static final int TO_MULTIPLIER = 3;
    private static final int MAX_TIMEOUT = 10000;
    private final MovingAverage average = new MovingAverage(0.2, SOCKET_TIMEOUT);
    private int timeout;

    // Constants exchanged with replica to manage client ID's
    private static final char HAVE_CLIENT_ID = 'F';
    private static final char REQUEST_NEW_ID = 'T';

    
    // List of replicas, and information who's the leader
    private final List<PID> replicas;

    private static final Random random = new Random();
    private final List<Integer> reconnectIds = new ArrayList<Integer>();

    // Two variables for numbering requests
    private long clientId = -1;
    private int sequenceId = 0;

    private Socket socket;
    private DataOutputStream output;
    private DataInputStream input;

    private final Integer contactReplicaId;

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * @param replicas - information about replicas to connect to
     * @deprecated Use {@link #Client(Configuration)}
     */
    public Client(List<PID> replicas) {
        this.replicas = replicas;
        contactReplicaId = null;
    }

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * @param config - the configuration with information about replicas to
     *            connect to
     * @throws IOException if I/O error occurs while reading configuration
     */
    public Client(Configuration config) throws IOException {
        this.replicas = config.getProcesses();
        contactReplicaId = null;
    }

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * Unless a redirect is received, uses ONLY the replica whose ID is declared
     * as contactReplicaId.
     * 
     * @param config - the configuration with information about replicas to
     *            connect to
     * @throws IOException if I/O error occurs while reading configuration
     */
    public Client(Configuration config, int contactReplicaId) throws IOException {
        this.contactReplicaId = contactReplicaId;
        this.replicas = config.getProcesses();
    }

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * Loads the configuration from the default configuration file, as defined
     * in the class {@link Configuration}
     * 
     * 
     * Unless a redirect is received, uses ONLY the replica whose ID is declared
     * as contactReplicaId.
     * 
     * @param config - the configuration with information about replicas to
     *            connect to
     * @throws IOException if I/O error occurs while reading configuration
     */
    public Client(int contactReplicaId) throws IOException {
        this.replicas = new Configuration().getProcesses();
        this.contactReplicaId = contactReplicaId;
    }

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * Loads the configuration from the default configuration file, as defined
     * in the class {@link Configuration}
     * 
     * @throws IOException if I/O error occurs while reading configuration
     */
    public Client() throws IOException {
        this(new Configuration());
    }

    /**
     * Sends request to replica, to execute service with specified object as
     * argument. This object should be known to replica, which generate reply.
     * This method will block until response from replica is received.
     * 
     * @param bytes - argument for service
     * @return reply from service
     * @throws ReplicationException if error occurs while sending request
     */
    public synchronized byte[] execute(byte[] bytes) throws ReplicationException {
        ClientRequest request = new ClientRequest(nextRequestId(), bytes);
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);

        long start = System.currentTimeMillis();

        while (true) {
            try {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Sending " + request.getRequestId());
                }

                ByteBuffer bb = ByteBuffer.allocate(command.byteSize());
                command.writeTo(bb);
                bb.flip();

                output.write(bb.array());
                output.flush();

                // Blocks only for socket timeout
                ClientReply clientReply = new ClientReply(input);

                switch (clientReply.getResult()) {
                    case OK:
                        Reply reply = new Reply(clientReply.getValue());
                        logger.fine("Reply OK");
                        assert reply.getRequestId().equals(request.getRequestId()) : "Bad reply. Expected: " +
                                                                                     request.getRequestId() +
                                                                                     ", got: " +
                                                                                     reply.getRequestId();

                        long time = System.currentTimeMillis() - start;
                        average.add(time);
                        // update socket timeout as 3 times average response time  
                        socket.setSoTimeout((int)(TO_MULTIPLIER * average.get()));
                        return reply.getValue();

                    case REDIRECT:
                        int currentPrimary = ByteBuffer.wrap(clientReply.getValue()).getInt();
                        if (currentPrimary < 0 || currentPrimary >= replicas.size()) {
                            // Invalid ID. Ignore redirect and try next replica.
                            logger.warning("Reply: Invalid redirect received: " + currentPrimary +
                                           ". Proceeding with next replica.");
                            currentPrimary = nextReplica();
                        } else {
                            logger.info("Reply REDIRECT to " + currentPrimary);
                        }
                        waitForReconnect(REDIRECT_TIMEOUT);
                        reconnect(currentPrimary);
                        break;

                    case NACK:
                        throw new ReplicationException("Nack received: " +
                                                       new String(clientReply.getValue()));

                    case BUSY:
                        throw new ReplicationException(new String(clientReply.getValue()));

                    default:
                        throw new RuntimeException("Unknown reply type");
                }

            } catch (SocketTimeoutException e) {
                logger.warning("Error waiting for answer: " + e.getMessage() + ", Request: " +
                               request.getRequestId());
                cleanClose();
                increaseTimeout();
                connect();
            } catch (IOException e) {
                logger.warning("Error reading socket: " + e.toString() + ". Request: " +
                               request.getRequestId());
                waitForReconnect(CONNECTION_FAILURE_TIMEOUT);
                connect();
            }
        }
    }

    public long getClientID() {
        return clientId;
    }

    /**
     * Tries to connect to a replica, cycling through the replicas until a
     * connection is successfully established. After successful connection, new
     * client id is granted which will be used for sending all messages.
     */
    public synchronized void connect() {
        reconnect(nextReplica());
    }

    private int nextReplica() {
        if (contactReplicaId != null) {
            return contactReplicaId;
        }

        if (reconnectIds.isEmpty()) {
            for (int i = 0; i < replicas.size(); ++i)
                reconnectIds.add(i);
            Collections.shuffle(reconnectIds, random);
        }
        return reconnectIds.remove(0);
    }

    private RequestId nextRequestId() {
        return new RequestId(clientId, ++sequenceId);
    }

    private void increaseTimeout() {
        average.add(Math.min(timeout * TO_MULTIPLIER, MAX_TIMEOUT));
    }

    /**
     * Tries to reconnect to a replica, cycling through the replicas until a
     * connection is successfully established.
     * 
     * @param replicaId try to connect to this replica
     */
    private void reconnect(int replicaId) {
        while (true) {
            int nr = -1;
            try {
                nr = nextReplica();
                connectTo(nr);
                // Success
                return;
            } catch (IOException e) {
                cleanClose();
                logger.warning("Connect to " + nr + " failed: " + e.getMessage());
                waitForReconnect(CONNECTION_FAILURE_TIMEOUT);
            }
        }
    }

    private void waitForReconnect(int timeout) {
        try {
            // random backoff
            timeout += random.nextInt(500);
            logger.warning("Reconnecting in " + timeout + "ms.");
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            logger.warning("Interrupted while sleeping: " + e.getMessage());
            // Set the interrupt flag again, it will result in an
            // InterruptException being thrown again the next time this thread
            // tries to block.
            Thread.currentThread().interrupt();
        }
    }

    private void cleanClose() {
        try {
            if (socket != null) {
                socket.shutdownOutput();
                socket.close();
                socket = null;
                logger.info("Closing socket");
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.log(Level.WARNING, "Not clean socket closing.");
        }
    }

    private void connectTo(int replicaId) throws IOException {
        // close previous connection if any
        cleanClose();

        PID replica = replicas.get(replicaId);

        String host = replica.getHostname();
        int port = replica.getClientPort();
        logger.info("Connecting to " + replica);
        socket = new Socket(host, port);

        timeout = (int) average.get() * TO_MULTIPLIER;
        timeout = Math.min(timeout, MAX_TIMEOUT);
        socket.setSoTimeout(timeout);
        socket.setReuseAddress(true);
        socket.setTcpNoDelay(true);
        output = new DataOutputStream(socket.getOutputStream());
        input = new DataInputStream(socket.getInputStream());

        initConnection();

        logger.info("Connected [p" + replicaId + "]. Timeout: " + socket.getSoTimeout());
    }

    /** Sends the contact replica our clientID or gets one from a replica */
    private void initConnection() throws IOException {
        if (clientId == -1) {
            output.write(REQUEST_NEW_ID);
            output.flush();
            clientId = input.readLong();
            logger.fine("New client id: " + clientId);
        } else {
            output.write(HAVE_CLIENT_ID);
            output.writeLong(clientId);
            output.flush();
        }
    }

    private final static Logger logger = Logger.getLogger(Client.class.getCanonicalName());
}

package lsr.paxos.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
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
import lsr.common.PrimitivesByteArray;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.ReplicationException;
import lsr.paxos.statistics.ClientStats;

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
 */
public class Client {
    /* Minimum time to wait before reconnecting after a connection failure 
     * (connection reset or refused).
     * In Paxos: must be large enough to allow the system to elect a new leader.
     * In SPaxos: can be short, since clients can connect to any replica. 
     */
    private static final int CONNECTION_FAILURE_TIMEOUT = 500;

    /* Minimum time to wait before reconnecting to a new replica after 
     * receiving a redirect 
     */ 
    private static final int REDIRECT_TIMEOUT = 100;

    /* How long to wait for an answer from the replica before connecting 
     * to another replica. 
     * 
     * In Paxos: Should be long enough for the replicas to suspect a failed replica 
     * and to elect a new leader
     * In SPaxos: can be short, since clients can connect to any replica.
     */
    private static final int SOCKET_TIMEOUT = 3000;

    // Connection timeout management - exponential moving average with upper
    // bound on max timeout. Timeout == TO_MULTIPLIER*average
    private static final int TO_MULTIPLIER = 3;
    private static final int MAX_TIMEOUT = 10000;
    private static final Random r = new Random();

    public static final String BENCHMARK_RUN_CLIENT = "BenchmarkRunClient";
    public static final boolean DEFAULT_BENCHMARK_RUN_CLIENT = false;
    public final boolean benchmarkRun;

    private final MovingAverage average = new MovingAverage(0.2, 2000);
    private int timeout;
    
    // List of replicas, and information who's the leader
    private final List<PID> replicas;
    private final int n;
    
    private int primary = -1;
    // Two variables for numbering requests
    private long clientId = -1;
    private int sequenceId = 0;
    
    private Socket socket;
    private DataOutputStream output;
    private DataInputStream input;
    private ClientStats stats;

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * @param replicas - information about replicas to connect to
     * @deprecated Use {@link #Client(Configuration)}
     */
    public Client(List<PID> replicas) {
        this.replicas = replicas;
        n = replicas.size();
        primary = r.nextInt(n);
        benchmarkRun = false;
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
        this.n = replicas.size();
        /* Randomize replica for initial connection. This avoids the thundering
         * herd problem when many clients are started simultaneously and all
         * connect to the same replicas.
         */
        primary = r.nextInt(n);
        this.benchmarkRun = config.getBooleanProperty(BENCHMARK_RUN_CLIENT,
                DEFAULT_BENCHMARK_RUN_CLIENT);
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

                // Blocks only for Socket.SO_TIMEOUT
                stats.requestSent(request.getRequestId());

                ClientReply clientReply = new ClientReply(input);


                switch (clientReply.getResult()) {
                    case OK:
                        Reply reply = new Reply(clientReply.getValue());
                        logger.fine("Reply OK");
                        assert reply.getRequestId().equals(request.getRequestId()) : 
                            "Bad reply. Expected: " + request.getRequestId() +
                            ", got: " + reply.getRequestId();

                        long time = System.currentTimeMillis() - start;
                        stats.replyOk(reply.getRequestId());
                        average.add(time);
                        return reply.getValue();

                    case REDIRECT:
                        int currentPrimary = PrimitivesByteArray.toInt(clientReply.getValue());
                        if (currentPrimary < 0 || currentPrimary >= n) {
                            // Invalid ID. Ignore redirect and try next replica.
                            logger.warning("Reply: Invalid redirect received: " + currentPrimary +
                                    ". Proceeding with next replica.");
                            currentPrimary = (primary + 1) % n;
                        } else {
                            stats.replyRedirect();
                            logger.info("Reply REDIRECT to " + currentPrimary);
                        }
                        waitForReconnect(REDIRECT_TIMEOUT);
                        reconnect(currentPrimary);
                        break;

                    case NACK:
                        throw new ReplicationException("Nack received: " +
                                new String(clientReply.getValue()));

                    case BUSY:
                        stats.replyBusy();
                        throw new ReplicationException(new String(clientReply.getValue()));

                    default:
                        throw new RuntimeException("Unknown reply type");
                }

            } catch (SocketTimeoutException e) {
                logger.warning("Error waiting for answer: " + e.getMessage() + ", Request: " + request.getRequestId() + ", node: " + primary);
                stats.replyTimeout();
                cleanClose();
                increaseTimeout();
                connect();
            } catch (IOException e) {
                logger.warning("Error reading socket: " + e.toString() + ". Request: " + request.getRequestId() + ", node: " + primary);
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
        reconnect((primary + 1) % n);
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
        int nextNode = replicaId;
        while (true) {
            try {
                connectTo(nextNode);
                // Success
                primary = nextNode;
                return;
            } catch (IOException e) {
                cleanClose();
                logger.warning("Connect to " + nextNode + " failed: " + e.getMessage());
                nextNode = (nextNode + 1) % n;
                waitForReconnect(CONNECTION_FAILURE_TIMEOUT);
            }
        }
    }

    private void waitForReconnect(int timeout) {
        try {
            // random backoff
            timeout += r.nextInt(500);
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
        
//        String host = "localhost";
        String host = replica.getHostname();
        int port = replica.getClientPort();        
        logger.info("Connecting to " + host + ":" + port);
        socket = new Socket(host, port);        

        timeout = (int) average.get() * TO_MULTIPLIER;
//        socket.setSoTimeout(Math.min(timeout, MAX_TIMEOUT));
        socket.setSoTimeout(SOCKET_TIMEOUT);
        socket.setReuseAddress(true);
        socket.setTcpNoDelay(true);
        output = new DataOutputStream(socket.getOutputStream());
        input = new DataInputStream(socket.getInputStream());

        initConnection();

        logger.info("Connected [p" + replicaId + "]. Timeout: " + socket.getSoTimeout());
    }

    private void initConnection() throws IOException {
        if (clientId == -1) {
            output.write('T'); // True
            output.flush();
            clientId = input.readLong();
            this.stats = benchmarkRun ? new ClientStats.ClientStatsImpl(clientId)
            : new ClientStats.ClientStatsNull();
            logger.fine("New client id: " + clientId);
        } else {
            output.write('F'); // False
            output.writeLong(clientId);
            output.flush();
        }
    }

    private final static Logger logger = Logger.getLogger(Client.class.getCanonicalName());
}

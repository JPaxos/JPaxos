package lsr.paxos.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import lsr.common.Config;
import lsr.common.Configuration;
import lsr.common.MovingAverage;
import lsr.common.PID;
import lsr.common.PrimitivesByteArray;
import lsr.common.Reply;
import lsr.common.Request;
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
    // List of replicas, and information who's the leader
    private final List<PID> replicas;
    private final int n;
    
    private int primary = -1;
    // Two variables for numbering requests
    private long clientId = -1;
    private int sequenceId = 0;

    // Connection timeout management - exponential moving average with upper
    // bound on max timeout. Timeout == TO_MULTIPLIER*average
    private static final int TO_MULTIPLIER = 5;
    private static final int MAX_TIMEOUT = 30000;
    private final MovingAverage average = new MovingAverage(0.2, 5000);
    private int timeout;

    /**
     * If couldn't connect so someone, how much time we wait before reconnecting
     * to other person
     */
    private static final long TIME_TO_RECONNECT = 1000;

    private Socket socket;
    private DataOutputStream output;
    private DataInputStream input;
    private boolean benchmarkRun = false;
    private ClientStats stats;

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * @param replicas - information about replicas to connect to
     */
    public Client(List<PID> replicas) {
        this.replicas = replicas;
        n = replicas.size();
        /*
         * Randomize replica for initial connection. This avoids the thundering
         * herd problem when many clients are started simultaneously and all
         * connect to the same replicas.
         */
        primary = (new Random()).nextInt(n);
    }

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * @param config - the configuration with information about replicas to
     *            connect to
     * @throws IOException if I/O error occurs while reading configuration
     */
    public Client(Configuration config) throws IOException {
        this(config.getProcesses());
        this.benchmarkRun = config.getBooleanProperty(Config.BENCHMARK_RUN, false);
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
        Request request = new Request(nextRequestId(), bytes);
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);

        while (true) {
            try {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Sending " + request.getRequestId());
                }

                if (Config.JAVA_SERIALIZATION) {
                    ByteArrayOutputStream prepare = new ByteArrayOutputStream();
                    (new ObjectOutputStream(prepare)).writeObject(command);
                    output.writeInt(prepare.size());
                    output.write(prepare.toByteArray());
                } else {
                    ByteBuffer bb = ByteBuffer.allocate(command.byteSize());
                    command.writeTo(bb);
                    bb.flip();
                    output.write(bb.array());
                }
                output.flush();

                // Blocks only for Socket.SO_TIMEOUT
                stats.requestSent(request.getRequestId());
                long start = System.currentTimeMillis();

                ClientReply clientReply;
                if (Config.JAVA_SERIALIZATION) {
                    try {
                        clientReply = (ClientReply) ((new ObjectInputStream(input)).readObject());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    clientReply = new ClientReply(input);
                }

                long time = System.currentTimeMillis() - start;

                switch (clientReply.getResult()) {
                    case OK:
                        Reply reply = new Reply(clientReply.getValue());
                        logger.fine("Reply OK");
                        assert reply.getRequestId().equals(request.getRequestId()) : "Bad reply. Expected: " +
                                                                                     request.getRequestId() +
                                                                                     ", got: " +
                                                                                     reply.getRequestId();

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
                        waitForReconnect();
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
                logger.warning("Timeout waiting for answer: " + e.getMessage());
                stats.replyTimeout();
                cleanClose();
                increaseTimeout();
                connect();
            } catch (IOException e) {
                logger.warning("Error reading socket: " + e.getMessage());
                connect();
            }
        }
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
                waitForReconnect();
            }
        }
    }

    private void waitForReconnect() {
        try {
            logger.warning("Reconnecting in " + TIME_TO_RECONNECT + "ms.");
            Thread.sleep(TIME_TO_RECONNECT);
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
        logger.info("Connecting to " + replica);
        socket = new Socket(replica.getHostname(), replica.getClientPort());

        timeout = (int) average.get() * TO_MULTIPLIER;
        socket.setSoTimeout(Math.min(timeout, MAX_TIMEOUT));
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
            logger.info("New client id: " + clientId);
        } else {
            output.write('F'); // False
            output.writeLong(clientId);
            output.flush();
        }
    }

    private final static Logger logger = Logger.getLogger(Client.class.getCanonicalName());
}

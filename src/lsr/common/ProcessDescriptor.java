package lsr.common;

import java.util.logging.Logger;

/**
 * Contains all the information describing the local process, including the
 * local id and the configuration of the system.
 * 
 * @author Nuno Santos (LSR)
 */
public final class ProcessDescriptor {
    public final Configuration config;

    /*---------------------------------------------
     * The following properties are read from the 
     * paxos.properties file  
     *---------------------------------------------*/
    /**
     * Defines the default window size - that is, the maximum number of
     * concurrently proposed instances.
     */
    public static final String WINDOW_SIZE = "WindowSize";
    public static final int DEFAULT_WINDOW_SIZE = 2;

    /**
     * Maximum UDP packet size in java is 65507. Higher than that and the send
     * method throws an exception.
     * 
     * In practice, most networks have a lower limit on the maximum packet size
     * they can transmit. If this limit is exceeded, the lower layers will
     * usually fragment the packet, but in some cases there's a limit over which
     * large packets are simply dropped or raise an error.
     * 
     * A safe value is the maximum ethernet frame: 1500 - maximum Ethernet
     * payload 20/40 - ipv4/6 header 8 - UDP header.
     * 
     * Usually values up to 8KB are safe.
     */
    public static final String MAX_UDP_PACKET_SIZE = "MaxUDPPacketSize";
    public static final int DEFAULT_MAX_UDP_PACKET_SIZE = 8 * 1024;

    /**
     * Protocol to use between replicas. TCP, UDP or Generic, which combines
     * both
     */
    public static final String NETWORK = "Network";
    public static final String DEFAULT_NETWORK = "TCP";

    /**
     * The maximum size of batched request.
     */
    public static final String BATCH_SIZE = "BatchSize";
    public static final int DEFAULT_BATCH_SIZE = 65507;

    /** How long to wait until suspecting the leader. In milliseconds */
    public static final String FD_SUSPECT_TO = "FDSuspectTimeout";
    public static final int DEFAULT_FD_SUSPECT_TO = 1000;

    /** Interval between sending heartbeats. In milliseconds */
    public final static String FD_SEND_TO = "FDSendTimeout";
    public static final int DEFAULT_FD_SEND_TO = 500;

    /**
     * The crash model used. For valid entries see {@link CrashModel}
     */
    public static final String CRASH_MODEL = "CrashModel";
    public static final CrashModel DEFAULT_CRASH_MODEL = CrashModel.FullSS;

    /**
     * Location of the stable storage (JPaxos logs)
     */
    public static final String LOG_PATH = "LogPath";
    public static final String DEFAULT_LOG_PATH = "jpaxosLogs";

    /**
     * Maximum time in ms that a batch can be delayed before being proposed.
     * Used to aggregate several requests on a single proposal, for greater
     * efficiency. (Naggle's algorithm for state machine replication).
     */
    public static final String MAX_BATCH_DELAY = "MaxBatchDelay";
    public static final int DEFAULT_MAX_BATCH_DELAY = 10;

    public static final String CLIENT_ID_GENERATOR = "ClientIDGenerator";
    public static final String DEFAULT_CLIENT_ID_GENERATOR = "TimeBased";

    /** Enable or disable collecting of statistics */
    public static final String BENCHMARK_RUN_REPLICA = "BenchmarkRunReplica";
    public static final boolean DEFAULT_BENCHMARK_RUN_REPLICA = false;

    /**
     * Before any snapshot was made, we need to have an estimate of snapshot
     * size. Value given as for now is 1 KB
     */
    public static final String FIRST_SNAPSHOT_SIZE_ESTIMATE = "FirstSnapshotEstimateBytes";
    public static final int DEFAULT_FIRST_SNAPSHOT_SIZE_ESTIMATE = 1024;

    /** Minimum size of the log before a snapshot is attempted */
    public static final String SNAPSHOT_MIN_LOG_SIZE = "MinLogSizeForRatioCheckBytes";
    public static final int DEFAULT_SNAPSHOT_MIN_LOG_SIZE = 100 * 1024;

    /** Ratio = \frac{log}{snapshot}. How bigger the log must be to ask */
    public static final String SNAPSHOT_ASK_RATIO = "SnapshotAskRatio";
    public static final double DEFAULT_SNAPSHOT_ASK_RATIO = 1;

    /** Ratio = \frac{log}{snapshot}. How bigger the log must be to force */
    public static final String SNAPSHOT_FORCE_RATIO = "SnapshotForceRatio";
    public static final double DEFAULT_SNAPSHOT_FORCE_RATIO = 2;

    /** Minimum number of instances for checking ratios */
    public static final String MIN_SNAPSHOT_SAMPLING = "MinimumInstancesForSnapshotRatioSample";
    public static final int DEFAULT_MIN_SNAPSHOT_SAMPLING = 50;

    public static final String RETRANSMIT_TIMEOUT = "RetransmitTimeoutMilisecs";
    public static final long DEFAULT_RETRANSMIT_TIMEOUT = 1000;

    /** If a TCP connection fails, how much to wait for another try */
    public static final String TCP_RECONNECT_TIMEOUT = "TcpReconnectMilisecs";
    public static final long DEFAULT_TCP_RECONNECT_TIMEOUT = 1000;

    /** ??? In milliseconds */
    public final static String CLIENT_BATCH_ACK_TIMEOUT = "replica.ClientBatchAckTimeout";
    public final static int DEFAULT_CLIENT_BATCH_ACK_TIMEOUT = 50;

    /** ??? Corresponds to a ethernet frame */
    public final static String FORWARD_MAX_BATCH_SIZE = "replica.ForwardMaxBatchSize";
    public final static int DEFAULT_FORWARD_MAX_BATCH_SIZE = 1450;

    /** ??? In milliseconds */
    public final static String FORWARD_MAX_BATCH_DELAY = "replica.ForwardMaxBatchDelay";
    public final static int DEFAULT_FORWARD_MAX_BATCH_DELAY = 20;

    /** How many selector threads to use */
    public static final String SELECTOR_THREADS = "replica.SelectorThreads";
    public static final int DEFAULT_SELECTOR_THREADS = -1;

    /**
     * Size of a buffer for reading client requests; larger requests than this
     * size will cause extra memory allocation and freeing at each such request.
     * This variable impacts memory usage, as each client connection
     * pre-allocates such buffer.
     */
    public static final String CLIENT_REQUEST_BUFFER_SIZE = "replica.ClientRequestBufferSize";
    public static final int DEFAULT_CLIENT_REQUEST_BUFFER_SIZE = 8 * 1024 + ClientCommand.HEADERS_SIZE;

    /*
     * Exposing fields is generally not good practice, but here they are made
     * final, so there is no danger of exposing them. Advantage: less
     * boilerplate code.
     */
    public final int localId;
    public final int numReplicas;
    public final int windowSize;
    public final int batchingLevel;
    public final int maxUdpPacketSize;
    public final int maxBatchDelay;
    public final String clientIDGenerator;
    public final String network;
    public final CrashModel crashModel;
    public final String logPath;
    public final int firstSnapshotSizeEstimate;
    public final int snapshotMinLogSize;
    public final double snapshotAskRatio;
    public final double snapshotForceRatio;
    public final int minSnapshotSampling;
    public final long retransmitTimeout;
    public final long tcpReconnectTimeout;
    public final int fdSuspectTimeout;
    public final int fdSendTimeout;

    public final int batchManagerAckTimeout;

    public final int forwardBatchMaxSize;
    public final int forwardBatchMaxDelay;

    public final int selectorThreadCount;

    public final int clientRequestBufferSize;

    /** ⌊(n+1)/2⌋ */
    public final int majority;

    /**
     * The singleton instance of process descriptor. Must be initialized before
     * use.
     */
    public static ProcessDescriptor processDescriptor = null;

    public static void initialize(Configuration config, int localId) {
        ProcessDescriptor.processDescriptor = new ProcessDescriptor(config, localId);
    }

    private ProcessDescriptor(Configuration config, int localId) {
        this.localId = localId;
        this.config = config;

        this.numReplicas = config.getN();

        this.windowSize = config.getIntProperty(
                WINDOW_SIZE, DEFAULT_WINDOW_SIZE);
        this.batchingLevel = config.getIntProperty(
                BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.maxUdpPacketSize = config.getIntProperty(
                MAX_UDP_PACKET_SIZE, DEFAULT_MAX_UDP_PACKET_SIZE);
        this.maxBatchDelay = config.getIntProperty(
                MAX_BATCH_DELAY, DEFAULT_MAX_BATCH_DELAY);
        this.clientIDGenerator = config.getProperty(
                CLIENT_ID_GENERATOR, DEFAULT_CLIENT_ID_GENERATOR);
        this.network = config.getProperty(
                NETWORK, DEFAULT_NETWORK);
        this.logPath = config.getProperty(
                LOG_PATH, DEFAULT_LOG_PATH);
        this.firstSnapshotSizeEstimate = config.getIntProperty(
                FIRST_SNAPSHOT_SIZE_ESTIMATE, DEFAULT_FIRST_SNAPSHOT_SIZE_ESTIMATE);
        this.snapshotMinLogSize = Math.max(1, config.getIntProperty(
                SNAPSHOT_MIN_LOG_SIZE, DEFAULT_SNAPSHOT_MIN_LOG_SIZE));
        this.snapshotAskRatio = config.getDoubleProperty(
                SNAPSHOT_ASK_RATIO, DEFAULT_SNAPSHOT_ASK_RATIO);
        this.snapshotForceRatio = config.getDoubleProperty(
                SNAPSHOT_FORCE_RATIO, DEFAULT_SNAPSHOT_FORCE_RATIO);
        this.minSnapshotSampling = config.getIntProperty(
                MIN_SNAPSHOT_SAMPLING, DEFAULT_MIN_SNAPSHOT_SAMPLING);
        this.retransmitTimeout = config.getLongProperty(
                RETRANSMIT_TIMEOUT, DEFAULT_RETRANSMIT_TIMEOUT);
        this.tcpReconnectTimeout = config.getLongProperty(
                TCP_RECONNECT_TIMEOUT, DEFAULT_TCP_RECONNECT_TIMEOUT);
        this.fdSuspectTimeout = config.getIntProperty(
                FD_SUSPECT_TO, DEFAULT_FD_SUSPECT_TO);
        this.fdSendTimeout = config.getIntProperty(
                FD_SEND_TO, DEFAULT_FD_SEND_TO);

        this.batchManagerAckTimeout = config.getIntProperty(CLIENT_BATCH_ACK_TIMEOUT,
                DEFAULT_CLIENT_BATCH_ACK_TIMEOUT);

        this.forwardBatchMaxDelay = processDescriptor.config.getIntProperty(
                FORWARD_MAX_BATCH_DELAY,
                DEFAULT_FORWARD_MAX_BATCH_DELAY);
        this.forwardBatchMaxSize = processDescriptor.config.getIntProperty(FORWARD_MAX_BATCH_SIZE,
                DEFAULT_FORWARD_MAX_BATCH_SIZE);

        this.selectorThreadCount = processDescriptor.config.getIntProperty(SELECTOR_THREADS,
                DEFAULT_SELECTOR_THREADS);

        this.clientRequestBufferSize = processDescriptor.config.getIntProperty(
                CLIENT_REQUEST_BUFFER_SIZE,
                DEFAULT_CLIENT_REQUEST_BUFFER_SIZE);

        String crash = config.getProperty(
                CRASH_MODEL, DEFAULT_CRASH_MODEL.toString());
        CrashModel crashModel;
        try {
            crashModel = CrashModel.valueOf(crash);
        } catch (IllegalArgumentException e) {
            crashModel = DEFAULT_CRASH_MODEL;
            logger.severe("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            logger.severe("Config file contains unknown crash model \"" + crash +
                          "\". Falling back to " + crashModel);
            logger.severe("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
        this.crashModel = crashModel;

        majority = (numReplicas + 1) / 2;

        printProcessDescriptor(config, crashModel);
    }

    private void printProcessDescriptor(Configuration config, CrashModel crashModel) {
        logger.config(config.toString());

        logger.config("Configuration: " + WINDOW_SIZE + "=" + windowSize + ", " +
                      BATCH_SIZE + "=" + batchingLevel + ", " + MAX_BATCH_DELAY +
                      "=" + maxBatchDelay + ", " + MAX_UDP_PACKET_SIZE + "=" +
                      maxUdpPacketSize + ", " + NETWORK + "=" + network + ", " +
                      CLIENT_ID_GENERATOR + "=" + clientIDGenerator);
        logger.config("Failure Detection: " + FD_SEND_TO + "=" + fdSendTimeout + ", " +
                      FD_SUSPECT_TO + "=" + fdSuspectTimeout);
        logger.config("Crash model: " + crashModel + ", LogPath: " + logPath);
        logger.config(
            FIRST_SNAPSHOT_SIZE_ESTIMATE + "=" + firstSnapshotSizeEstimate + ", " +
                    SNAPSHOT_MIN_LOG_SIZE + "=" + snapshotMinLogSize + ", " +
                    SNAPSHOT_ASK_RATIO + "=" + snapshotAskRatio + ", " +
                    SNAPSHOT_FORCE_RATIO + "=" + snapshotForceRatio + ", " +
                    MIN_SNAPSHOT_SAMPLING + "=" + minSnapshotSampling
            );

        logger.config(
            RETRANSMIT_TIMEOUT + "=" + retransmitTimeout + ", " +
                    TCP_RECONNECT_TIMEOUT + "=" + tcpReconnectTimeout
            );

        logger.config(CLIENT_BATCH_ACK_TIMEOUT + "=" + batchManagerAckTimeout);

        logger.config(FORWARD_MAX_BATCH_DELAY + "=" + forwardBatchMaxDelay);
        logger.config(FORWARD_MAX_BATCH_SIZE + "=" + forwardBatchMaxSize);

        logger.config(SELECTOR_THREADS + "=" + forwardBatchMaxSize);

        logger.config(CLIENT_REQUEST_BUFFER_SIZE + "=" + clientRequestBufferSize);

    }

    /**
     * @return the local process
     */
    public PID getLocalProcess() {
        return config.getProcess(localId);
    }

    public int getLeaderOfView(int view) {
        return view % numReplicas;
    }

    public boolean isLocalProcessLeader(int view) {
        return getLeaderOfView(view) == localId;
    }

    private final static Logger logger = Logger.getLogger(ProcessDescriptor.class.getCanonicalName());
}

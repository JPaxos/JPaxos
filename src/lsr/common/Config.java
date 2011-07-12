package lsr.common;

import lsr.paxos.replica.Replica.CrashModel;

public class Config {
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
    public static final CrashModel DEFAULT_CRASH_MODEL = CrashModel.FullStableStorage;

    /**
     * Location of the stable storage (JPaxos logs)
     */
    public static final String LOG_PATH = "LogPath";
    public static final String DEFAULT_LOG_PATH = "jpaxosLogs";

    /**
     * If <code>taskQueue</code> grows to more than this value, the system is
     * considered as being busy. This is used to refuse additional work from
     * clients, thus preventing the queues from growing too much.
     */
    public static final String BUSY_THRESHOLD = "BusyThreshold";
    public static final int DEFAULT_BUSY_THRESHOLD = 10 * 1024;

    /**
     * Maximum time in ms that a batch can be delayed before being proposed.
     * Used to aggregate several requests on a single proposal, for greater
     * efficiency. (Naggle's algorithm for state machine replication).
     */
    public static final String MAX_BATCH_DELAY = "MaxBatchDelay";
    public static final int DEFAULT_MAX_BATCH_DELAY = 10;

    /**
     * Indicates, if the underlying service is deterministic. A deterministic
     * one may always share logs. Other should not do this, as results of
     * processing the same messages may be different
     */
    public static final boolean DEFAULT_MAY_SHARE_SNAPSHOTS = true;
    public static final String MAY_SHARE_SNAPSHOTS = "MayShareSnapshots";

    public static final String CLIENT_ID_GENERATOR = "ClientIDGenerator";
    public static final String DEFAULT_CLIENT_ID_GENERATOR = "TimeBased";

    /** Enable or disable collecting of statistics */
    public static final String BENCHMARK_RUN_REPLICA = "BenchmarkRunReplica";
    public static final boolean DEFAULT_BENCHMARK_RUN_REPLICA = false;
    
    public static final String BENCHMARK_RUN_CLIENT = "BenchmarkRunClient";
    public static final boolean DEFAULT_BENCHMARK_RUN_CLIENT = false;

    /**
     * Before any snapshot was made, we need to have an estimate of snapshot
     * size. Value given as for now is 1 KB
     */
    public static final String FIRST_SNAPSHOT_SIZE_ESTIMATE = "FirstSnapshotEstimateBytes";
    public static final int DEFAULT_FIRST_SNAPSHOT_SIZE_ESTIMATE = 1024;

    /** Minimum size of the log before a snapshot is attempted */
    public static final String SNAPSHOT_MIN_LOG_SIZE = "MinLogSizeForRatioCheckBytes";
    public static final int DEFAULT_SNAPSHOT_MIN_LOG_SIZE = 10 * 1024;

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

    /** This is the timeout designed for periodic Catch-Up */
    public static final String PERIODIC_CATCHUP_TIMEOUT = "PeriodicCatchupMilisecs";
    public static final long DEFAULT_PERIODIC_CATCHUP_TIMEOUT = 2000;

    /** If a TCP connection fails, how much to wait for another try */
    public static final String TCP_RECONNECT_TIMEOUT = "TcpReconnectMilisecs";
    public static final long DEFAULT_TCP_RECONNECT_TIMEOUT = 1000;
    
    /** How many selector threads to use */
    public static final String SELECTOR_THREADS= "SelectorThreads";
    public static final int DEFAULT_SELECTOR_THREADS = -1;    

    /*---------------------------------------------
     * The following properties are compile time 
     * constants.
     *---------------------------------------------*/
    /**
     * If enabled, all objects are transformed into byte[] or I/O streams using
     * java's object input/output streams.
     * 
     * Otherwise user defined functions are used for that.
     */
    public static final boolean JAVA_SERIALIZATION = false;

    public static final int UDP_RECEIVE_BUFFER_SIZE = 64 * 1024;

    public static final int UDP_SEND_BUFFER_SIZE = 64 * 1024;

    /** for re-sending catch-up query we use a separate, self-adjusting timeout */
    public static final long CATCHUP_MIN_RESEND_TIMEOUT = 50;
}

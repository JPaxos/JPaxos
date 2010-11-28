package lsr.common;

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
     * 1500 - maximum ethernet payload 20/40 - ipv4/6 header 8 - udp header. The
     * IP layer will fragment larger packets. Usually, it is very efficient.
     */
    public static final String MAX_UDP_PACKET_SIZE = "MaxUDPPacketSize";
    // Maximum UDP packet size in java is 65507. Higher than that and
    // the send method throws an exception.
    public static final int DEFAULT_MAX_UDP_PACKET_SIZE = 65507;

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

    /**
     * If <code>_taskQueue</code> grows to more than this value, the system is
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
    public static final String BENCHMARK_RUN = "BenchmarkRun";
    public static final boolean DEFAULT_BENCHMARK_RUN = false;

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
    public static final boolean javaSerialization = false;

    /**
     * Before any snapshot was made, we need to have an estimate of snapshot
     * size. Value given as for now is 1 KB
     */
    public static final double firstSnapshotSizeEstimate = 1024;

    /** Minimum size of the log before a snapshot is attempted */
    public static final int SNAPSHOT_MIN_LOG_SIZE = 10 * 1024;

    /** Ratio = \frac{log}{snapshot}. How bigger the log must be to ask */
    public static final double SNAPSHOT_ASK_RATIO = 1;

    /** Ratio = \frac{log}{snapshot}. How bigger the log must be to force */
    public static final double SNAPSHOT_FORCE_RATIO = 2;

    /** Minimum number of instances for checking ratios */
    public static final int MIN_SNAPSHOT_SAMPLING = 50;

    public static final int UDP_RECEIVE_BUFFER_SIZE = 64 * 1024;
    public static final int UDP_SEND_BUFFER_SIZE = 64 * 1024;

    public static final long RETRANSMIT_TIMEOUT = 1000;

    /** for re-sending catch-up query we use a separate, self-adjusting timeout */
    public static final long CATCHUP_MIN_RESEND_TIMEOUT = 50;

    /** This is the timeout designed for periodic Catch-Up */
    public static final long PERIODIC_CATCHUP_TIMEOUT = 2000;

    /** If a TCP connection fails, how much to wait for another try */
    public static final long TCP_RECONNECT_TIMEOUT = 1000;

}

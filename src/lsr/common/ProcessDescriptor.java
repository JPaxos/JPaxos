package lsr.common;

import java.util.logging.Logger;

import lsr.paxos.replica.Replica;
import lsr.paxos.replica.Replica.CrashModel;

/**
 * Contains all the information describing the local process, including the
 * local id and the configuration of the system.
 * 
 * @author Nuno Santos (LSR)
 */
public final class ProcessDescriptor {
    public final Configuration config;

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
    public final int busyThreshold;
    public final boolean mayShareSnapshots;
    public final int maxBatchDelay;
    public final String clientIDGenerator;
    public final boolean benchmarkRunReplica;
    public final boolean benchmarkRunClient;
    public final String network;
    public final Replica.CrashModel crashModel;
    public final String logPath;

    public final int firstSnapshotSizeEstimate;
    public final int snapshotMinLogSize;
    public final double snapshotAskRatio;
    public final double snapshotForceRatio;
    public final int minSnapshotSampling;
    public final long retransmitTimeout;
    public final long periodicCatchupTimeout;
    public final long tcpReconnectTimeout;
    public final int fdSuspectTimeout;
    public final int fdSendTimeout;

    /*
     * Singleton class with static access. This allows any class on the JVM to
     * statically access the process descriptor without needing to be given a
     * reference.
     */
    private static ProcessDescriptor instance;

    public static void initialize(Configuration config, int localId) {
        ProcessDescriptor.instance = new ProcessDescriptor(config, localId);
    }

    public static ProcessDescriptor getInstance() {
        return instance;
    }

    private ProcessDescriptor(Configuration config, int localId) {
        this.localId = localId;
        this.config = config;

        this.numReplicas = config.getN();

        this.windowSize = config.getIntProperty(Config.WINDOW_SIZE, Config.DEFAULT_WINDOW_SIZE);
        this.batchingLevel = config.getIntProperty(Config.BATCH_SIZE, Config.DEFAULT_BATCH_SIZE);
        this.maxUdpPacketSize = config.getIntProperty(Config.MAX_UDP_PACKET_SIZE,
                Config.DEFAULT_MAX_UDP_PACKET_SIZE);
        this.busyThreshold = config.getIntProperty(Config.BUSY_THRESHOLD,
                Config.DEFAULT_BUSY_THRESHOLD);
        this.mayShareSnapshots = config.getBooleanProperty(Config.MAY_SHARE_SNAPSHOTS,
                Config.DEFAULT_MAY_SHARE_SNAPSHOTS);
        this.maxBatchDelay = config.getIntProperty(Config.MAX_BATCH_DELAY,
                Config.DEFAULT_MAX_BATCH_DELAY);
        this.clientIDGenerator = config.getProperty(Config.CLIENT_ID_GENERATOR,
                Config.DEFAULT_CLIENT_ID_GENERATOR);
        this.benchmarkRunReplica = config.getBooleanProperty(Config.BENCHMARK_RUN_REPLICA,
                Config.DEFAULT_BENCHMARK_RUN_REPLICA);
        this.benchmarkRunClient = config.getBooleanProperty(Config.BENCHMARK_RUN_CLIENT,
                Config.DEFAULT_BENCHMARK_RUN_CLIENT);
        this.network = config.getProperty(Config.NETWORK, Config.DEFAULT_NETWORK);

        this.logPath = config.getProperty(Config.LOG_PATH, Config.DEFAULT_LOG_PATH);

        String defCrash = Config.DEFAULT_CRASH_MODEL.toString();
        String crash = config.getProperty(Config.CRASH_MODEL, defCrash);
        CrashModel crashModel;
        try {
            crashModel = Replica.CrashModel.valueOf(crash);
        } catch (IllegalArgumentException e) {
            crashModel = Config.DEFAULT_CRASH_MODEL;
            logger.severe("");
            logger.severe("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            logger.severe("Config file contains unknown crash model \"" + crash + "\"");
            logger.severe("Falling back to " + crashModel);
            logger.severe("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            logger.severe("");
        }
        this.crashModel = crashModel;

        this.firstSnapshotSizeEstimate = config.getIntProperty(
                Config.FIRST_SNAPSHOT_SIZE_ESTIMATE,
                Config.DEFAULT_FIRST_SNAPSHOT_SIZE_ESTIMATE);
        this.snapshotMinLogSize = Math.max(1, config.getIntProperty(Config.SNAPSHOT_MIN_LOG_SIZE,
                Config.DEFAULT_SNAPSHOT_MIN_LOG_SIZE));
        this.snapshotAskRatio = config.getDoubleProperty(Config.SNAPSHOT_ASK_RATIO,
                Config.DEFAULT_SNAPSHOT_ASK_RATIO);
        this.snapshotForceRatio = config.getDoubleProperty(Config.SNAPSHOT_FORCE_RATIO,
                Config.DEFAULT_SNAPSHOT_FORCE_RATIO);
        this.minSnapshotSampling = config.getIntProperty(Config.MIN_SNAPSHOT_SAMPLING,
                Config.DEFAULT_MIN_SNAPSHOT_SAMPLING);
        this.retransmitTimeout = config.getLongProperty(Config.RETRANSMIT_TIMEOUT,
                Config.DEFAULT_RETRANSMIT_TIMEOUT);
        this.periodicCatchupTimeout = config.getLongProperty(Config.PERIODIC_CATCHUP_TIMEOUT,
                Config.DEFAULT_PERIODIC_CATCHUP_TIMEOUT);
        this.tcpReconnectTimeout = config.getLongProperty(Config.TCP_RECONNECT_TIMEOUT,
                Config.DEFAULT_TCP_RECONNECT_TIMEOUT);

        this.fdSuspectTimeout = config.getIntProperty(Config.FD_SUSPECT_TO,
                Config.DEFAULT_FD_SUSPECT_TO);
        this.fdSendTimeout = config.getIntProperty(Config.FD_SEND_TO,
                Config.DEFAULT_FD_SEND_TO);

        logger.config("Configuration: " + Config.WINDOW_SIZE + "=" + windowSize + ", " +
                       Config.BATCH_SIZE + "=" + batchingLevel + ", " + Config.MAX_BATCH_DELAY +
                       "=" + maxBatchDelay + ", " + Config.MAX_UDP_PACKET_SIZE + "=" +
                       maxUdpPacketSize + ", " + Config.NETWORK + "=" + network + ", " +
                       Config.BUSY_THRESHOLD + "=" + busyThreshold + ", " +
                       Config.MAY_SHARE_SNAPSHOTS + "=" + mayShareSnapshots + ", " +
                       Config.BENCHMARK_RUN_REPLICA + "=" + benchmarkRunReplica + ", " +
                       Config.BENCHMARK_RUN_CLIENT + "=" + benchmarkRunClient + ", " +
                       Config.CLIENT_ID_GENERATOR + "=" + clientIDGenerator);
        logger.config("Failure Detection: " + Config.FD_SEND_TO + "=" + fdSendTimeout + ", " +
                      Config.FD_SUSPECT_TO + "=" + fdSuspectTimeout);
        logger.config("Crash model: " + crashModel + ", LogPath: " + logPath);
        logger.config(
            Config.FIRST_SNAPSHOT_SIZE_ESTIMATE + "=" + firstSnapshotSizeEstimate + ", " +
                    Config.SNAPSHOT_MIN_LOG_SIZE + "=" + snapshotMinLogSize + ", " +
                    Config.SNAPSHOT_ASK_RATIO + "=" + snapshotAskRatio + ", " +
                    Config.SNAPSHOT_FORCE_RATIO + "=" + snapshotForceRatio + ", " +
                    Config.MIN_SNAPSHOT_SAMPLING + "=" + minSnapshotSampling
            );

        logger.config(
            Config.RETRANSMIT_TIMEOUT + "=" + retransmitTimeout + ", " +
                    Config.PERIODIC_CATCHUP_TIMEOUT + "=" + periodicCatchupTimeout + ", " +
                    Config.TCP_RECONNECT_TIMEOUT + "=" + tcpReconnectTimeout
            );

    }

    /**
     * 
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

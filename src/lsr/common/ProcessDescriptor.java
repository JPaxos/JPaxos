package lsr.common;

import java.util.logging.Logger;

/**
 * Contains all the information describing the local process, including the
 * local id and the configuration of the system.
 * 
 * @author Nuno Santos (LSR)
 */
public class ProcessDescriptor {
	public final Configuration config;

	/* Exposing fields is generally not good practice,
	 * but here they are made final, so there is no danger of
	 * exposing them. Advantage: less boilerplate code. 
	 */	
	public final int localID;
	public final int windowSize;
	public final int batchingLevel;
	public final int maxUdpPacketSize;
	public final int busyThreshold;
	public final boolean mayShareSnapshots;
	public final int maxBatchDelay;
	public final String clientIDGenerator;
	public final boolean benchmarkRun;

	/*
	 * Singleton class with static access. This allows any class on the JVM to
	 * statically access the process descriptor without needing to be given a
	 * reference.
	 */
	private static ProcessDescriptor instance;

	public static void initialize(Configuration config, int localId) {
		assert instance == null : "ProcessDescriptor already initialized. Only one instance allowed.";
		ProcessDescriptor.instance = new ProcessDescriptor(config, localId);
	}

	public static ProcessDescriptor getInstance() {
		return instance;
	}

	private ProcessDescriptor(Configuration config, int localId) {
		this.localID = localId;
		this.config = config;

		this.windowSize =
			config.getIntProperty(Config.WINDOW_SIZE,
			                      Config.DEFAULT_WINDOW_SIZE);
		this.batchingLevel =
			config.getIntProperty(Config.BATCH_SIZE,
			                      Config.DEFAULT_BATCH_SIZE);
		this.maxUdpPacketSize =
			config.getIntProperty(Config.MAX_UDP_PACKET_SIZE,
			                      Config.DEFAULT_MAX_UDP_PACKET_SIZE);
		this.busyThreshold =
			config.getIntProperty(Config.BUSY_THRESHOLD,
			                      Config.DEFAULT_BUSY_THRESHOLD);
		this.mayShareSnapshots =
			config.getBooleanProperty(Config.MAY_SHARE_SNAPSHOTS,
			                          Config.DEFAULT_MAY_SHARE_SNAPSHOTS);
		this.maxBatchDelay =
			config.getIntProperty(Config.MAX_BATCH_DELAY,
			                      Config.DEFAULT_MAX_BATCH_DELAY);
		this.clientIDGenerator = 
			config.getProperty(Config.CLIENT_ID_GENERATOR,
			                   Config.DEFAULT_CLIENT_ID_GENERATOR);		
		this.benchmarkRun = 
			config.getBooleanProperty(Config.BENCHMARK_RUN,
			                          Config.DEFAULT_BENCHMARK_RUN);
		
		_logger.config("Configuration: " + 
		               Config.WINDOW_SIZE + "=" + windowSize + 
		               ", " + Config.BATCH_SIZE + "=" + batchingLevel + 
		               ", " + Config.MAX_BATCH_DELAY + "=" + maxBatchDelay +
		               ", " + Config.MAX_UDP_PACKET_SIZE + "=" + maxUdpPacketSize + 
		               ", " + Config.BUSY_THRESHOLD + "=" + busyThreshold + 
		               ", " + Config.MAY_SHARE_SNAPSHOTS + "=" + mayShareSnapshots +
		               ", " + Config.BENCHMARK_RUN + "=" + benchmarkRun +
		               ", " + Config.CLIENT_ID_GENERATOR + "=" + clientIDGenerator);
	}

	/**
	 * 
	 * @return the local process
	 */
	public PID getLocalProcess() {
		return config.getProcess(localID);
	}

	private final static Logger _logger =
		Logger.getLogger(ProcessDescriptor.class.getCanonicalName());
}

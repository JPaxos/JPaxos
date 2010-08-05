package lsr.common;

import java.util.logging.Logger;

/**
 * Contains all the information describing the local process, including the
 * local id and the configuration of the system.
 * 
 * @author Nuno Santos (LSR)
 */
public class ProcessDescriptor {
	public final int localID;
	public final Configuration config;

	public final int windowSize;
	public final int batchingLevel;
	public final int maxUdpPacketSize;
	public final int busyThreshold;

	public ProcessDescriptor(Configuration config, int localId) {
		this.localID = localId;
		this.config = config;

		this.windowSize = config.getIntProperty(Config.WINDOW_SIZE,
				Config.DEFAULT_WINDOW_SIZE);
		this.batchingLevel = config.getIntProperty(Config.BATCH_SIZE,
				Config.DEFAULT_BATCH_SIZE);
		this.maxUdpPacketSize = config.getIntProperty(
				Config.MAX_UDP_PACKET_SIZE, Config.DEFAULT_MAX_UDP_PACKET_SIZE);
		this.busyThreshold = config.getIntProperty(Config.BUSY_THRESHOLD,
				Config.DEFAULT_BUSY_THRESHOLD);

		_logger.config("Configuration: " + "WindowSize: " + windowSize + ", "
				+ "BatchSize: " + batchingLevel + ", " + "MaxUDPPacketSize: "
				+ maxUdpPacketSize + ", " + "BusyThreshold: " + busyThreshold);
	}

	/**
	 * 
	 * @return the local process
	 */
	public PID getLocalProcess() {
		return config.getProcess(localID);
	}

	private final static Logger _logger = Logger
			.getLogger(ProcessDescriptor.class.getCanonicalName());
}

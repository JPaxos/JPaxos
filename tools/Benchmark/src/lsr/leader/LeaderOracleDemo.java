package lsr.leader;

import java.util.logging.Logger;


import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.leader.latency.LatencyDetector;
import lsr.leader.latency.LatencyDetectorListener;
import lsr.leader.latency.SimpleLatencyDetector;
import lsr.paxos.network.UdpNetwork;

/**
 * Simple demo for a leader election oracle
 * 
 * @author Donzé Benjamin
 * @author Nuno Santos (LSR)
 */
public class LeaderOracleDemo implements LeaderOracleListener, LatencyDetectorListener {

	public LatencyLeaderOracle llo;
	public LatencyDetector latencyDetector;

	public double[] localRTT;
	public Object lock = new Object();

	
	public LeaderOracleDemo(Configuration config, int localId) throws Exception {
		// Start by reading the list of nodes from a file. By default: "nodes.conf"

		SingleThreadDispatcher executor = new SingleThreadDispatcher("leader");
		ProcessDescriptor p = new ProcessDescriptor(config, localId);
		
		// Use UDP to send the leader election messages
		UdpNetwork network = new UdpNetwork(p);
		latencyDetector = new SimpleLatencyDetector(p, network, executor);
		latencyDetector.registerLatencyDetectorListener(this);

		// Create the leader oracle
		llo = new LatencyLeaderOracle(p, network, executor, latencyDetector);
		llo.registerLeaderOracleListener(this);

		// Start the oracle
		llo.start();
	}

	/**
	 * Called whenever a new leader is elected. This implementation simply
	 * prints a notification on the screen, but a real protocol would perform
	 * some action in reaction to the change on leader.
	 */
	@Override
	public void onNewLeaderElected(int leader) {

	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			usage();
			System.exit(1);
		}

		int localID = Integer.parseInt(args[0]);

		Configuration p = args.length == 2 ? new Configuration(args[1]) : new Configuration();
		LeaderOracleDemo lod = new LeaderOracleDemo(p, localID);
	}

	private static void usage() {
		System.out.println(
				"Invalid arguments. Usage:\n" + 
		"   java lsr.leader.LeaderOracleDemo <replicaID> [conf directory]");
	}


	@Override
	public void onNewRTTVector(double[] rttVector) {
		synchronized (lock) {
			localRTT = rttVector;
		}
	}
	
	private final static Logger _logger = Logger.getLogger(
			LeaderOracleSimulation.class.getCanonicalName());
}

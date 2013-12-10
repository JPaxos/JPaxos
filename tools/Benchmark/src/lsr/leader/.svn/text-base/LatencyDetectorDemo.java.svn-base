package lsr.leader;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.leader.latency.LatencyDetector;
import lsr.leader.latency.LatencyDetectorListener;
import lsr.leader.latency.SimpleLatencyDetector;
import lsr.paxos.network.UdpNetwork;

/**
 * Simple demo for the local latency detector
 * 
 * @author Donzé Benjamin
 *
 */
public class LatencyDetectorDemo implements LatencyDetectorListener {
	private final LatencyDetector latencyDetector;

	public LatencyDetectorDemo(Configuration config, int localId) throws Exception {
		
		ProcessDescriptor p = new ProcessDescriptor(config, localId);
		// Use UDP to send the leader election messages
		UdpNetwork network = new UdpNetwork(p);
		SingleThreadDispatcher executor = new SingleThreadDispatcher("Leader");
		
		// Create the latency detector
		latencyDetector = new SimpleLatencyDetector(p, network, executor);
		latencyDetector.registerLatencyDetectorListener(this);
		
		// Start the oracle
		latencyDetector.start();
		_logger.info("Latency detector started");
		
		executor.schedule(new Runnable() {
			@Override
			public void run() {
				try {
					latencyDetector.stop();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, 30, TimeUnit.SECONDS);
		
		executor.schedule(new Runnable() {
			@Override
			public void run() {
				try {
					latencyDetector.start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, 45, TimeUnit.SECONDS);
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			usage();
			System.exit(1);
		}		
		int localID = Integer.parseInt(args[0]);
		Configuration p = new Configuration();
		// LatencyDetectorDemo tester = 
			new LatencyDetectorDemo(p, localID);
	}
	
	@Override
	public void onNewRTTVector(double[] rttVector) {
		System.out.println("New RTT vector: " + Arrays.toString(rttVector));
	}

	private static void usage() {
		System.out.println(
				"Invalid arguments. Usage:\n" + 
				"   java lsr.leader.latency.LatencyDetectorDemo <replicaID>");
	}
	
	private final static Logger _logger = Logger.getLogger(LatencyDetectorDemo.class.getCanonicalName());
}

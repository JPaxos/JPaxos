package lsr.leader;

import helpers.ProcessCrashController;
import helpers.ProcessCrashController.Reply;
import helpers.ProcessCrashController.Request;
import helpers.ProcessCrashController.Type;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;
import java.util.logging.Logger;

import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.leader.BasicLeaderOracle;
import lsr.leader.LatencyLeaderOracle;
import lsr.leader.LeaderOracle;
import lsr.leader.LeaderOracleListener;
import lsr.leader.latency.SimpleLatencyDetector;
import lsr.paxos.network.UdpNetwork;

/**
 * Application for simulation purpose. 
 * 
 * @author Donzé Benjamin
 * @author Nuno Santos
 *
 */
public class LeaderOracleSimulation implements LeaderOracleListener {
	
	private static final String ORACLE_TYPE = "simulation.OracleType";
	
	public final Object lock = new Object();
	private final UdpNetwork network;
	private final ProcessDescriptor p;
	private final SingleThreadDispatcher executor;

	abstract class LeaderOracleController {
		abstract public void start() throws Exception;
		abstract public void stop() throws Exception;
		
		abstract public LeaderOracle getLeaderOracle();
		
		public void printRunParameters(PrintWriter pw) throws IOException {
			pw.println("N          & " + p.config.getN() + "\\\\");
			pw.println("Leader     & " + getLeaderOracle().getClass().getName() + "\\\\");
			pw.println("Delta      & " + getLeaderOracle().getDelta() + "\\\\");
		}
		
	}
	
	class LatencyLOController extends LeaderOracleController {
		public LatencyLeaderOracle leaderOracle;
		public SimpleLatencyDetector latencyDetector;
		
		@Override
		public void start() throws Exception {
			latencyDetector = new SimpleLatencyDetector(p, network, executor);
			// Create the leader oracle
			leaderOracle = new LatencyLeaderOracle(p, network, executor, latencyDetector);
			leaderOracle.registerLeaderOracleListener(LeaderOracleSimulation.this);			
			latencyDetector.start();			
			leaderOracle.start();
		}

		@Override
		public void stop() throws Exception {
			leaderOracle.stop();
			latencyDetector.stop();
			
			leaderOracle = null;
			latencyDetector = null;
		}
		
		@Override
		public void printRunParameters(PrintWriter pw) throws IOException {
			super.printRunParameters(pw);
			pw.println("Epsilon    & " + leaderOracle.getEpsilon() + "\\\\");
			pw.println("SendPeriod & " + latencyDetector.getSendPeriod());
		}

		@Override
		public LeaderOracle getLeaderOracle() {
			return leaderOracle;
		}
	}
	
	class BasicLOController extends LeaderOracleController {
		public BasicLeaderOracle leaderOracle;
		
		@Override
		public void start() throws Exception {
			// Create the leader oracle
			leaderOracle = new BasicLeaderOracle(p, network, executor);
			leaderOracle.start();
		}

		@Override
		public void stop() throws Exception {
			leaderOracle.stop();			
			leaderOracle = null;
		}
		
		public LeaderOracle getLeaderOracle() {
			return leaderOracle;
		}
	}
	
	
	private final LeaderOracleController loController; 
	
	public LeaderOracleSimulation(Configuration config, int localId)
	throws Exception
	{
		p = new ProcessDescriptor(config, localId);
		// Use UDP to send the leader election messages
		network = new UdpNetwork(p);
		executor = new SingleThreadDispatcher("leader");
		String oracleType = config.getProperty(ORACLE_TYPE, "LatencyLeaderOracle");
		if (oracleType.equals("LatencyLeaderOracle")) {
			loController = new LatencyLOController();
		} else if (oracleType.equals("BasicLeaderOracle")) {
			loController = new BasicLOController();
		} else {
			throw new Exception("Unknown oracle type: " + oracleType);
		}
		_logger.info("Using " + oracleType);
	}

	/**
	 * Called whenever a new leader is elected. This implementation simply
	 * prints a notification on the screen, but a real protocol would perform
	 * some action in reaction to the change on leader.
	 */
	@Override
	public void onNewLeaderElected(int leader) {
		_logger.info("New leader elected: " + leader);
	}


	public void runSimulation() throws Exception {
		// Connect to the lock server that controls the number of crashes
		Socket s = new Socket("localhost", ProcessCrashController.PORT);
		ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		
		// Initialize
		System.out.println("Initializing");
		oos.writeObject(
				new Request(Type.Initialize, p.localID, p.config.getN()));
		Reply answer = (Reply) ois.readObject();
		System.out.println("Initialized");
		if (answer.type != Type.Grant) {
			throw new Exception("Could not initialize: " + answer.msg);
		}
		
		loController.start();
		
		// Only the first process writes the run parameters
		if (p.localID == 0) {
			PrintWriter pw = new PrintWriter(new FileWriter("run-params.log"));		
			loController.printRunParameters(pw);
			pw.close();
		}
		
//		Random random = new Random();
//		int sr = random.nextInt(60000*6);
		
		Random random = new Random();
//		long time_next_restart = 60000 * 1;
		while(true) {
			// Generate a random number with a gaussian of mean 6 minutes and std-dev 1 minute
//			double rn = random.nextGaussian();
//			rn = rn * 60000 + 60000*6;
//			long time_next_crash = Math.round(rn);
			// Crash local process every 5 to 10 seconds
			long nextCrash = 5000 + random.nextInt(5000);
			
			try {
				// Wait until time of crash				
				Thread.sleep(nextCrash);	
			
				_logger.info("Requesting crash");
				// Request crash
				oos.writeObject(
						new Request(Type.Crash, p.localID, p.config.getN()));
				oos.flush();
				answer = (Reply) ois.readObject();
				if (answer.type != Type.Grant) {
					_logger.info("Crash denied: " + answer.msg);
					// Skip crash
					continue;
				}
				// Stop
				loController.stop();
				Thread.sleep(10000);

				oos.writeObject(
						new Request(Type.Recover, p.localID, p.config.getN()));
				answer = (Reply) ois.readObject();
				if (answer.type != Type.Grant) {
					throw new RuntimeException("Could not recover: " + answer.msg);
				}
				loController.start();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}

	/**
	 * Arguments:  <repNo> [config.file]
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			usage();
			System.exit(1);
		}
		int localID = Integer.parseInt(args[0]);
		Configuration p = args.length == 2 ?  
				new Configuration(args[1]) : new Configuration();
		
		LeaderOracleSimulation lod = new LeaderOracleSimulation(p, localID);
		
		lod.runSimulation();
	}	

	private static void usage() {
		System.out.println(
				"Invalid arguments. Usage:\n" + 
				"   java lsr.leader.LeaderOracleSimulation <replicaID> [conf directory]");
	}
	
	private final static Logger _logger = Logger.getLogger(LeaderOracleSimulation.class.getCanonicalName());	
}

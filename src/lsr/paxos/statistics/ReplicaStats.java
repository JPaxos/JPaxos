package lsr.paxos.statistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.TreeMap;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.paxos.statistics.Analyzer.Instance;

public class ReplicaStats {
	/** Singleton */
	private static ReplicaStats instance;	
	public static ReplicaStats initialize(int n, int localID) throws IOException {
		assert instance == null : "Already initialized";
		if (ProcessDescriptor.getInstance().benchmarkRun) {
			instance = new ReplicaStatsImpl(n, localID);
		} else {
			instance = new ReplicaStats();
		}
		return instance;
	}
	public static ReplicaStats getInstance() {
		return instance;
	}

	/*
	 * Empty implementation. For non-benchmark runs
	 */
	public void consensusStart(int cid, int size, int k, int alpha)	{}
	public void retransmit(int cid) {}
	public void consensusEnd(int cid) {}
	public void advanceView(int newView) {}

}

/*
 * Full implementation.
 */
class ReplicaStatsImpl extends ReplicaStats {
	private int n;
	private int localID;
	// Current view of each process
	private int view = -1;
	private final Writer consensusStats;

	private final TreeMap<Integer, Instance> instances = 
		new TreeMap<Integer, Instance>();


	boolean firstLog = true;

	ReplicaStatsImpl(int n, int localID) throws IOException {
		this.n = n;
		this.localID = localID;		
		consensusStats = new FileWriter(new File("replica-"+localID+".stats.log"));		
		consensusStats.write("% Consensus\t" + Instance.getHeader() + "\n");
	}

	//	private void batchDone(BatchDone curLog) {
	//		// Current round of the process
	//		processRound[curLog.p] = curLog.rid;
	//		
	//		Round round = rounds.get(curLog.rid);
	//		if (round == null) {
	//			round = new Round(curLog.getTimestamp());
	//			rounds.put(curLog.rid, round);
	//		}
	//	}

	//	private void requestReceived(RequestReceived curLog) {
	//		
	//	}

	//	private void requestExecuted(RequestExecuted curLog) {
	//		Interval round = rounds.get(curLog.round);
	//		assert round != null : "Round finished before starting: " + curLog;
	//		
	//		ProcessStats stat = stats[curLog.p];
	//		stat.finishReason[curLog.reason.ordinal()]++;
	//		stat.roundCount++;
	//		
	//		round.nFinished++;
	//		if (round.nFinished == 1) {
	//			round.firstEnd = curLog.getTimestamp();
	//		} 
	//		if (round.nFinished == n) {
	//			round.lastEnd = curLog.getTimestamp();
	//		}
	//		
	//		assert round.nFinished < 5 : "Too many processes finished round " + curLog.round;		
	//	}

	/* (non-Javadoc)
	 * @see lsr.paxos.statistics.IReplicaStats2#consensusStart(int, int, int, int)
	 */
	@Override
	public void consensusStart(int cid, int size, int k, int alpha) {
		//		System.out.println(localID + " consensusStart-" + cid);
		// Ignore logs from non-leader
		assert isLeader() : "Not leader. cid: " + cid;
		//		if (!isLeader()) { 
		//			return;
		//		}

		//		Instance cInstance = instances.get(cid);
		assert !instances.containsKey(cid) : "Instance not null: " + instances.get(cid);
		Instance cInstance = new Instance(System.nanoTime(), size, k, alpha);
		instances.put(cid, cInstance);
	}

	/* (non-Javadoc)
	 * @see lsr.paxos.statistics.IReplicaStats2#retransmit(int)
	 */
	@Override
	public void retransmit(int cid) {
		assert isLeader() : "Not leader. cid: " + cid;
		//		if (!isLeader()) { 
		//			return;
		//		}

		Instance instance = instances.get(cid);
		instance.retransmit++;
	}

	/**
	 * True if the process considers itself the leader
	 * @return
	 */
	private boolean isLeader() {		
		return view % n == localID;
	}

	/* (non-Javadoc)
	 * @see lsr.paxos.statistics.IReplicaStats2#consensusEnd(int)
	 */
	@Override
	public void consensusEnd(int cid) {
		// Ignore log if process is not leader.
		if (!isLeader()) { 
			return;
		}
		//		System.out.println(localID + " consensusEnd-" + cInstance + " this.cid="+ this.cId + ", cid=" + cid);
		Instance cInstance = instances.remove(cid);		
		assert cInstance != null : "Instance not started: " +  cid;
		//		if (instance == null) {
		//			// Ignore, this is not the primary otherwise there would have 
		//			// been a consensus start log before
		//			return;
		//		}

		cInstance.end = System.nanoTime();		
		// Write to log
		writeInstance(cid, cInstance);
	}

	/* (non-Javadoc)
	 * @see lsr.paxos.statistics.IReplicaStats2#advanceView(int)
	 */
	@Override
	public void advanceView(int newView) {		
		_logger.warning("[RepStats] View: " + view + "->" + newView);
		this.view = newView;		
		for (Integer cid : instances.keySet()) {
			Instance cInstance = instances.get(cid);
			cInstance.end = -1;
			writeInstance(cid, cInstance);		
		}
		instances.clear();
	}

	private void writeInstance(int cId, Instance cInstance) {
		try {
			consensusStats.write(cId + "\t" + cInstance + "\n");
			consensusStats.flush();
		}catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}	
	}

	private final static Logger _logger = Logger.getLogger(ReplicaStats.class
	                                                       .getCanonicalName());
}

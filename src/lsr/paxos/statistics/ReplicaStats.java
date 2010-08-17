package lsr.paxos.statistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.TreeMap;

import lsr.paxos.statistics.Analyzer.Instance;

public class ReplicaStats {
	private static final String STATS_FILE = "stats.txt";

	private int n;
	private int localID;
	// Current view of each process
	private int view = -1;
	private final Writer consensusStats;

	private final TreeMap<Integer, Instance> instances = 
		new TreeMap<Integer, Instance>();

	/** Singleton */
	private static ReplicaStats instance;	
	public static ReplicaStats initialize(int n, int localID) throws IOException {
		assert instance == null : "Already initialized";		
		instance = new ReplicaStats(n, localID);
		return instance;
	}

	public static ReplicaStats getInstance() {
		return instance;
	}

	boolean firstLog = true;

	private ReplicaStats(int n, int localID) throws IOException {
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

	public void consensusStart(int cid, int size, int k) {
		//		System.out.println(localID + " consensusStart-" + cid);
		// Ignore logs from non-leader
		if (!isLeader()) { 
			return;
		}

		//		Instance cInstance = instances.get(cid);
		assert !instances.containsKey(cid) : "Instance not null: " + instances.get(cid);
		Instance cInstance = new Instance(System.nanoTime());
		cInstance.valueSize = size;
		cInstance.nRequests = k;
		instances.put(cid, cInstance);
	}

	public void retransmit(int cid) {
		if (!isLeader()) { 
			return;
		}		

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

	public void consensusEnd(int cid) {
		// Ignore log if process is not leader.
		if (!isLeader()) { 
			return;
		}
		//		System.out.println(localID + " consensusEnd-" + cInstance + " this.cid="+ this.cId + ", cid=" + cid);
		Instance cInstance = instances.remove(cid);		
		assert instance != null : "Instance not started: " +  instance;
		//		if (instance == null) {
		//			// Ignore, this is not the primary otherwise there would have 
		//			// been a consensus start log before
		//			return;
		//		}

		cInstance.end = System.nanoTime();		
		// Write to log
		writeInstance(cid, cInstance);
	}

	public void advanceView(int newView) {		
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
}

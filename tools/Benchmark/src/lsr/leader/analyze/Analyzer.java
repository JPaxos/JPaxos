package lsr.leader.analyze;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import lsr.common.Util;

public class Analyzer {
	static class Interval implements Comparable<Interval> {
		public final long start;
		public final long end;
		public Interval(long curGPStart, long date) {
			this.start = curGPStart;
			this.end = date;
		}

		@Override
		public int compareTo(Interval o) {
			long thisVal = this.start;
			long anotherVal = o.start;
			return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
		}

		public long getDuration() {
			return end-start;
		}
		
		public static String getHeader() {
			return "Start\tEnd\tDuration";
		}

		@Override
		public String toString() {
			return start + "\t" + end + "\t" + getDuration();
		}
	}

	enum State {
		Initial,
		Up,
		Crashed,
		Recovering
	}


	static class RTTPeriod {
		public final long startTS;
		public final double majrtts[];
		public final int leader;

		//		/** The leader RTT during this period */
		//		public final double leaderRTT;
		//		/** How far is leader from optimal */
		//		public final double gamma;

		public RTTPeriod(long startTS, double majrtts[], int leader) {
			this.startTS = startTS;
			this.majrtts = majrtts;
			this.leader = leader;
		}

		public double getLeaderMajRTT() {
			if (leader == -1) {
				return -1;
			} else {
				return majrtts[leader];
			}
		}
		
		public double getGamma() {
			if (leader == -1) {
				return -1;
			} else {
				double gamma = Double.MIN_VALUE;
				for (double rtt : majrtts) {
					gamma = Math.max(majrtts[leader]- rtt, gamma); 
				}
				return gamma;
			}
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(startTS).append("\t");
			for (double rtt : majrtts) {
				sb.append(Util.toString(rtt)).append("\t");
			}
			sb.append(leader);
			return sb.toString();
		}

		public static String getHeader(int nProcs) {
			StringBuilder s = new StringBuilder();
			s.append("Start\t");
			for (int i = 0; i < nProcs; i++) {
				s.append("p"+i+"\t");
			}
			s.append("Leader");
			return s.toString();
		}
	}

	// State of a process. Recovering 
	class ProcessState {
		public State state = State.Initial;
		public final int id;
		public int view = 0;

		public double[] rtt;
		public double majRtt = -1;

		public ProcessState(int n, int id) {
			this.id = id;
			this.rtt = new double[n];
			Arrays.fill(rtt, Double.MAX_VALUE);
		}

		public int getLeader() {
			return view % n;
		}

		@Override
		public String toString() {
			return "p" + id + " View="+ view + ", state="+state + ", rtt=" + Util.toString(rtt) + "(" + majRtt + ")";
		}
	}
	
	// Output files
	/* all the logs and analysis of relevant events of the run.
	 * Intended for interpretation of the results and debugging.
	 */
	private final static String RESULTS_DIR = ".";
	
	private final String RUNLOG_FILE = "log.txt";
	private final PrintStream runLog;
	
	private final String STATS_FILE = "stats.txt";	
	private final String SUSPICIONS_FILE = "suspicions.txt";
	private final String ELECTION_FILE = "elections.txt";
	private final String GOODPERIODS_FILE = "goodperiods.txt";
	private final String RTTS_FILE = "rtts.txt";

	
	private final List<LogRecord> logList;
	private final int n;

	// State of the system
	private final ProcessState[] system;

	// The current log being analyzed
	private LogRecord curLog;
	// Timestamp of the next event
	private long nextEvtTS;	

	// Metrics to be measured
	private long runStart = -1;
	private long runEnd = -1;
	private long leaderAvailableTime = 0;	

	//////////////////
	// Suspicion times
	//////////////////	
	// Auxiliary: Time when the last good period started. -1 means bad period.
	private long curGPStart = -1;
	private int curLeader = -1;
	// Time series with duration of good periods
	private final List<Interval> goodPeriods = new ArrayList<Interval>();

	//////////////////
	// Suspicion times
	//////////////////

	// Auxiliary variable
	// Time when the last leader crashed
	private long leaderCrashTime;
	// The last leader that crashed, -1 if there is a leader.
	private int crashedLeader;
	// When the leader was first suspected.
	private long suspectTime;

	private long falseSuspicions = 0;
	private long trueSuspicions = 0;

	private long promotions = 0;

	// Time series  
	private List<Interval> suspectTimes = new ArrayList<Interval>();	
	private List<Interval> electionTimes = new ArrayList<Interval>();

	private List<RTTPeriod> rttIntervals = new ArrayList<RTTPeriod>();

	public Analyzer(List<LogRecord> logList, int n) throws FileNotFoundException {
		this.logList = logList;
		this.n = n;
		this.system = new ProcessState[n];
		for (int i = 0; i < system.length; i++) {
			system[i] = new ProcessState(n, i);
		}
//		runLog = System.out;
		runLog = new PrintStream(new File(RESULTS_DIR, RUNLOG_FILE));
	}

	public void analyze() throws FileNotFoundException {
		log("Starting analyzis");

		// Should be 0 if logs are normalized
		runStart = logList.get(0).getTimestamp();
		runEnd = logList.get(logList.size()-1).getTimestamp();

		double majrtts[] = new double[n];
		Arrays.fill(majrtts, Double.MAX_VALUE);
		rttIntervals.add(new RTTPeriod(runStart, majrtts, curLeader));

		for (int i = 0; i < logList.size(); i++) {
			curLog = logList.get(i);

			// If we are at the end, use the current event 
			// timestamp as the end of the run.
			int j = i+1 < logList.size() ? i+1 : i;
			nextEvtTS = logList.get(j).getTimestamp();
			System.out.println("Analyzing: " + curLog);
			log(curLog.toString());
			
			switch (curLog.type) {
			case Start:
				start((Start)curLog);
				break;

			case Stop:
				stop((Stop)curLog);
				break;

			case BetterLeader:
				betterLeader((BetterLeader)curLog);
				break;

			case Suspect:
				suspect((Suspect)curLog);
				break;

			case RTT:
				rtt((RTT)curLog);
				break;

			case TimeReply:
				timeReply((TimeReply)curLog);
				break;

			case View:
				newView((NewView)curLog);
				break;

			default:
				throw new AssertionError("Unknown log type: " + curLog);
			}
			updateMetrics();
		}
		finishRun();

		printMetrics();
	}


	/**
	 * Performs the final computations at the end of the run
	 */
	private void finishRun() {
		// Close the last good period
		if (curGPStart != -1) {
			goodPeriods.add(new Interval(curGPStart, runEnd));
			curGPStart = -1;

			onGPEnd();										
		}
	}


	/**
	 * Checks if there is an elected leader. 
	 * A process p is a leader if 
	 * - p is alive
	 * - all processes that are up are in the same view as the leader
	 * 
	 * @return The current leader of the system or -1 if there is no leader
	 */
	private int getLeader() {
		int higestView = -1;
		for (ProcessState ps : system) {
			// only consider alive processes
			if (ps.state == State.Up) {
				if (higestView == -1) {
					// This is the first alive process 
					higestView = ps.view;
					assert ps.view >= 0;
				}
				// There is a candidate leader.
				// All alive processes must have it as leader
				if (higestView != ps.view) {
					return -1;
				}
			}
		}
		int leader = higestView % n;
		if (higestView != -1 && system[leader].state == State.Up) {
			return leader;
		} else { 
			return -1;
		}
	}


	private void updateMetrics() {
		if (getLeader() != -1) {
			leaderAvailableTime += nextEvtTS - curLog.getTimestamp();
		}
	}

	/** 
	 * Should be called only after the state is updated 
	 * with the new log and before processing the next log
	 */
	private void updateGoodPeriod() {
		/* possibilities:
		 * 1. A new GP starts.
		 * 2. A GP ends
		 * 3. Continue on a bad period
		 * 4. Continue on a GP. Not possible, because the leader 
		 * changed in one process.
		 */
		int leader = getLeader();
		if (curGPStart == -1) {
			// not in a good period
			if (leader  >= 0) {
				// A good period started
				curGPStart = curLog.getTimestamp();
				curLeader = leader;
				onGPStart();				
			} // else still on a bad period. 

		} else {
			// In a GP. It may continue or it may end.
			if (leader == -1) { 
				goodPeriods.add(new Interval(curGPStart, curLog.getTimestamp()));
				curGPStart = -1;
				curLeader = -1;
				onGPEnd();								
			} else {
				assert this.curLeader == leader : "A GP should not start immediately after the previous";
			}
		}
	}

	private String aliveProcesses() {
		StringBuffer sb = new StringBuffer();
		for (ProcessState ps : system) {
			if (ps.state == State.Up) {
				sb.append(ps.id).append(" ");
			}
		}
		return sb.toString().trim();
	}

	private void newView(NewView log) {
		assert system[log.p].state != State.Crashed : "Crashed process cannot install a new view";

		// A process is no longer recovering when 
		// it installs a view higher than 0.
		// TODO: it's also not recovering if it remains on view 0 but 
		// receives ALIVEs from leader. Or leader is on view 0.
		if (system[log.p].state == State.Recovering && log.view != 0) {
			log(log.p + " Recovering -> Up");
			system[log.p].state = State.Up; 
		}			

		// The leader might have changed. Must recheck if it is a good period.		
		system[log.p].view = log.view;
		updateGoodPeriod();

		if (suspectTime != -1 && getLeader() >= 0) {
			log("Recovery complete");
			electionTimes.add(new Interval(suspectTime, log.getTimestamp()));
			suspectTime = -1;
		}

		updateRTTs();
	}


	private void timeReply(TimeReply log) {
		// TODO Auto-generated method stub

	}

	private void rtt(RTT log) {
		ProcessState ps =system[log.p]; 
		ps.rtt = log.rttVector;
		ps.majRtt = log.majRtt;
		//		System.out.println(log.getTimestamp() + " RTT(" + ps.id + ") " + Util.toString(ps.rtt) + " ("+ps.majRtt+")");
		double[] majs = new double[system.length];
		for (int i = 0; i < system.length; i++) {			
			majs[i] = system[i].majRtt;
		}
		log("MAJRTTs: " + Util.toString(majs));		
		updateRTTs();
	}

	private void log(String string) {
		if (curLog == null) {
			runLog.println(string);
		} else {
			runLog.println(curLog.getTimestamp() + " " + string);
		}
	}

	private void updateRTTs() {
		// Update the majrtts of the previous interval
		RTTPeriod prevInterval = rttIntervals.get(rttIntervals.size()-1);
		
		double[] majrtts = Arrays.copyOf(prevInterval.majrtts, prevInterval.majrtts.length);
		if (curLog instanceof RTT) {
			RTT rttLog = (RTT) curLog;
			majrtts[rttLog.p] = rttLog.majRtt;  
		} else {
			// the majrtts remain unchanged
			if (prevInterval.leader == curLeader) {
				// Neither the RTTs nor the leader changed. Skip.
				return;
			}
		}
		
		RTTPeriod rttPeriod = new RTTPeriod(
				curLog.getTimestamp(), majrtts,	curLeader);
		rttIntervals.add(rttPeriod);
		log(curLog.getTimestamp() + " " + rttPeriod);
	}

	private void suspect(Suspect log) {
		if (leaderCrashTime != -1 && crashedLeader == log.suspectedProc) {
			log("Leader suspected.");
			suspectTimes.add(new Interval(leaderCrashTime, log.getTimestamp()));			
			crashedLeader = -1;
			leaderCrashTime = -1;
			suspectTime = log.getTimestamp();

			trueSuspicions++;
		} 

		// There is an alive leader and log.p is suspecting it.
		if (curLeader != -1 && log.suspectedProc == curLeader) {
			log("FALSE SUSPICION");
			falseSuspicions++;
		}
	}

	private void betterLeader(BetterLeader log) {
		log("BETTERLEADER " +
				"Prev: " + curLeader + " (" + system[curLeader].majRtt + "), " +
				"New " + log.newLeader + " (" + system[log.newLeader].majRtt + ")");
		promotions++;
		//		onGPEnd();
		//		this.curGPLeader = log.newLeader;
		//		onGPStart();		
	}

	private void onGPStart() {
		log("GPSTART - Leader: " + curLeader + 
				", Alive: " + aliveProcesses());
	}

	private void onGPEnd() {
		log("GPEND - Alive: " + aliveProcesses());
	}


	private void stop(Stop log) {
		assert system[log.p].state != State.Crashed;		

		// Suspicion time: if the leader fails, count time until first suspicion
		if (log.p == getLeader()) {
			log("Leader failed. Waiting for suspicion.");
			assert leaderCrashTime == -1;
			leaderCrashTime = log.getTimestamp();
			crashedLeader = log.p;
		}

		// Since p crashed, it is no longer the leader
		system[log.p].state = State.Crashed;

		// Good period is over
		updateGoodPeriod();
	}


	private void start(Start log) {
		assert system[log.p].state == State.Initial || system[log.p].state == State.Crashed;

		if (system[log.p].state == State.Initial) {
			system[log.p].state = State.Up;
		} else {
			system[log.p].state = State.Recovering;
		}		

		if (log.p == crashedLeader) {
			// this is extremely unlikely to happen
			log("Leader recovered before being suspected.");
			leaderCrashTime = -1;
		}
	}

	private void printMetrics() throws FileNotFoundException {
		// General statistics first
		PrintStream statsPS = new PrintStream(new File(RESULTS_DIR, STATS_FILE));
		
		// Manually breaking it down is simple and would work as well.
		// But this requires fewer lines of code
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		// Must set time zone to neutral, to prevent automatic adjustments.
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		long runLength = runEnd - runStart;
		// Output as a latex table. To be included directly on a tex file.
		statsPS.println("RunStart: & " + runStart + " \\\\");
		statsPS.println("RunEnd:   & " + runEnd + " \\\\");
		statsPS.println("Duration: & " + runLength +  " (" + sdf.format(runLength) + ")\\\\");
		double leaderAvailable= (((double)leaderAvailableTime)/runLength)*100;
		statsPS.println("Availability:      & " + leaderAvailableTime + " (" + Util.toString(leaderAvailable) + "\\%) \\\\" );
		statsPS.println("CorrectSuspicions: & " + trueSuspicions + " \\\\");
		statsPS.println("FalseSuspicions:   & " + falseSuspicions + " \\\\");
		statsPS.println("Promotions:        & " + promotions + " \\\\");

		
		String[] header = new String[2];
		// Sum of good periods
		long totalGP = sum(goodPeriods);
		statsPS.println("Total Good Period: & " + totalGP + " \\\\");
		header[0] = "Good periods";
		header[1] = Interval.getHeader();
		printList(header, goodPeriods, GOODPERIODS_FILE);

		// Time waiting for suspecting a crashed process		
		long totalSuspect = sum(suspectTimes);
		statsPS.println("Suspect time:      & " + totalSuspect + " \\\\");
		header[0] = "Suspect periods";
		printList(header, suspectTimes, SUSPICIONS_FILE);

		long totalElectionTime = sum(electionTimes);
		statsPS.println("Election time:     & " + totalElectionTime);
		header[0] = "Election periods";
		printList(header, electionTimes, ELECTION_FILE);
		
		header[0] = "RTTs";
		header[1] = RTTPeriod.getHeader(n);			
		printList(header, rttIntervals, RTTS_FILE);
	}

	private static long sum(List<Interval> list) {
		long total = 0;
		for (Interval interval : list) {
			total += interval.getDuration();
		}
		return total;
	}

//	private void printTimeSeriesList(List<RTTPeriod> list, String file) 
//	throws FileNotFoundException 
//	{
//		PrintWriter pw = new PrintWriter(new File(RESULTS_DIR, file));
//		for (RTTPeriod r : list) {
//			StringBuilder sb = new StringBuilder();
//			sb.append(r.startTS);
//			for (double majrtt : r.majrtts) {
//				sb.append("\t").append(Util.toString(majrtt));				
//			}
//			sb.append("\t").append(r.leader);
//			sb.append("\t").append(Util.toString(r.getGamma()));
//			pw.println(sb);
//		}
//		pw.flush();
//	}

	private void printList(String header[], List<?> list, String outFile) 
	throws FileNotFoundException 
	{
		PrintStream ps = new PrintStream(new File(RESULTS_DIR, outFile));
		for (String s : header) {
			ps.println("%"+s);
		}
		for (Object r : list) {
			ps.println(r);
		}
		ps.close();
	}

//	private void printTimeSeries(List<Interval> list, String file) 
//	throws FileNotFoundException
//	{
//		PrintWriter pw = new PrintWriter(new File(RESULTS_DIR, file));		
//		for (Interval r : list) {
//			long ts = r.start - runStart;
//			pw.println(ts + "\t" + r.getDuration());
//		}
//		pw.flush();
//	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (ProcessState ps : system) {
			sb.append(ps + "\n");
		}
		return sb.toString();
	}
}

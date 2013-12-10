package lsr.leader.analyze;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lsr.common.Pair;

/**
 * Take a set of log files as parameter and analyze them.
 * 
 * @author Donz� Benjamin
 */
public class LogAnalyzer {
	private Pair<Integer,BufferedReader>[] logReaders;
	
	/** Timestamp of the analysis */
//	private final static Date timestamp = new Date();
	
	/** Where to place the result files */
	private final static File dir;
	static {
//		DateFormat df = new SimpleDateFormat("kkmmss");
//		dir = new File("run-"+ df.format(timestamp));
		dir = new File("run");
		dir.mkdir();		
	}

	// Number of replicas
	private final int N;

	// List of all the log records
	private final List<LogRecord> logList = new ArrayList<LogRecord>();
//	// List of the computed recovery times
//	private final List<Integer> lstRecoveryTimes = new ArrayList<Integer>();
//	// List of the computed first suspicion times
//	private final List<Integer> lstFirstSuspicionTimes = new ArrayList<Integer>();
//	// List of the computed reelection times
//	private final List<Integer> lstReelectionTimes = new ArrayList<Integer>();
//	// List of the time to reply
//	private final List<Integer> lstTimeToReplies = new ArrayList<Integer>();
//	// List of the computed wrong demotion times
//	private final List<Integer> lstWrongDemTimes = new ArrayList<Integer>();
//	// List of the computed good period durations
//	private final List<Integer> lstGoodPeriodDurations = new ArrayList<Integer>();
//	// List of the outputed majRTT
//	private final List<Double> lstMajRtt = new ArrayList<Double>();
//	// List of all the leader change because of better one founded event. First element of the pair is the current leader. Second is the selected leader
//	private final List<Pair<Integer, Integer>> lstBetterLeaderEvent = new LinkedList<Pair<Integer,Integer>>();

//	// Counter for the view changes.
//	private int nbViewChange = 0;
//	// Counter of the occurence of view change because of better one selection
//	private int nbBetterLeader = 0;
//	// Counters for useless and wrong better change.
//	// A change is useless a good process is demoted
//	// A change is wrong if a bad process is elected
//	private int nbBetterUseless = 0;
//	private int nbBetterWrong = 0;
//	// Counter of leader crash
//	private int nbLeaderCrash = 0;

	public LogAnalyzer(Pair<Integer,BufferedReader>[] logFiles) {
		this.logReaders = logFiles;
		this.N = logReaders.length; 
	}
	
	public void parseLogs() throws IOException {
		// Read all the files and store each log line in a LogRecord
		// All the LogRecords are stored in a list
		for(int i = 0; i < logReaders.length; i++) {			
			int procID = logReaders[i].getKey();
			BufferedReader reader = logReaders[i].getValue();
			System.out.println("Analysing log: " + logReaders[i]);

			while (true) {
				String logLine = reader.readLine();
				if (logLine == null) {
					break;
				}
				LogRecord nextLogRecord = LogRecord.parse(logLine, procID);
				if (nextLogRecord == null) {
//					System.out.println("Skipping");
				} else {					
					logList.add(nextLogRecord);
				}
			}
		}
		// Sort the list by increasing timestamp
		Collections.sort(logList);
		normalize(logList);
	}
	
	private void normalize(List<LogRecord> logList2) {
		long runStart = logList2.get(0).getTimestamp();
		for (LogRecord logRecord : logList2) {
			logRecord.subtract(runStart);
		}		
	}

	public void writeLogs() throws FileNotFoundException {
		PrintWriter[] pws = new PrintWriter[N];
		for (int i = 0; i < pws.length; i++) {
			pws[i] = new PrintWriter(new File(dir, i+".out")); 
		}
		PrintWriter all = new PrintWriter(new File(dir, "run.out"));

		for (LogRecord log : logList) {
			String aux = log.toString();
			pws[log.p].println(aux);
			all.println(aux);
		}

		for (PrintWriter pw : pws) {
			pw.close();
		}		
		all.close();
	}

	
//	public void analyze() {
//		// Number of currently alive processes
//		int nbAlive = 0;
//		// Set of the currently alive processes
//		Set<Integer> aliveProc = new TreeSet<Integer>();
//
//		// Current leader and view
//		int leader = -1;
//		int view = -1;
//
//		boolean computeTimeRecov = false;
//		boolean crashDetected = false;
//		boolean isSelectLChange = false;
//		// During crash recovery: expected correct view
//		int expectedView = 0;
//		// During crash recovery: set of recovered processes
//		Set<Integer> recoveredProc = new TreeSet<Integer>();
//		long timeOfCrash = 0;
//		long firstSuspicionTime = 0;
//		long tmpTimeOfRecovery = 0;
//
//		long lastWrongDemTime = 0;
//
//		boolean goodPeriod = false;
//		long startTimeGoodPeriod = 0;
//		Set<Integer> procUpToDate = new TreeSet<Integer>();
//
//		// The file rttVector will contain all the rttVector information when 
//		// useless of wrong better leader election occurs.
//		PrintWriter rttVecWriter = null;
//		try {
//			rttVecWriter = new PrintWriter(new File(dir, "rttVectors.out"));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		for(LogRecord nextRec: logList) {
//			switch (nextRec.getType()) {
//			case Start: {
//				Start start = (Start) nextRec;
//				// Add the started process to the list of the currently alive processes
//				nbAlive++;
//				aliveProc.add(start.getProc());
//				break;
//			}
//
//			case Stop: {
//				Stop stop = (Stop) nextRec;
//				// Remove the stopped process of the list of the currently alive processes.
//				nbAlive--;
//				aliveProc.remove(nextRec.getProc());
//
//				// If the leader crash
//				if(!computeTimeRecov && nextRec.getProc() == leader) {
//					// Now the analyzer have to compute the metrics about leader crash recovery
//					computeTimeRecov = true;
//					crashDetected = false;
//					expectedView = view + 1;
//
//					nbLeaderCrash++;
//					recoveredProc.clear();
//					timeOfCrash = nextRec.getTimestamp();
//					firstSuspicionTime = 0;
//
//					// If it was a good period then the leader crash end this good period.
//					if(goodPeriod) {
//						lstGoodPeriodDurations.add((int)(nextRec.getTimestamp() - startTimeGoodPeriod));
//					}
//					goodPeriod = false;
//					procUpToDate.clear();
//				}
//
//				// Treat the case when the algorithm try to recover after a leader crash 
//				// and that the only process which has not already recovered just crash 
//				if(computeTimeRecov && recoveredProc.containsAll(aliveProc)) {
//					lstRecoveryTimes.add((int)(tmpTimeOfRecovery - timeOfCrash));
//					lstReelectionTimes.add((int)(tmpTimeOfRecovery - firstSuspicionTime));
//					computeTimeRecov = false;
//
//					goodPeriod = true;
//					startTimeGoodPeriod = nextRec.getTimestamp();
//				}
//
//				// If the only process being not up to date (concerning the view) crash then a good period just start
//				if(!goodPeriod && procUpToDate.containsAll(aliveProc)) {
//					goodPeriod = true;
//					startTimeGoodPeriod = nextRec.getTimestamp();
//				}
//				break;
//			}
//			case View: 
//				NewView viewEvt = (NewView)nextRec;
//				if(viewEvt.view > view) {
//					// A new view is started for the first time
//					if(goodPeriod) {
//						lstGoodPeriodDurations.add((int)(nextRec.getTimestamp() - startTimeGoodPeriod));
//					}
//
//					// This designate the end of the good period because all processes have to change of view
//					goodPeriod = false;
//					procUpToDate.clear();
//
//					// If the algorithm is not trying to recover from a leader crash and that this view change isn't
//					// induced by the better leader election mechanism then this view change is a mistake.
//					if(!computeTimeRecov && isSelectLChange == false) {
//						if(lastWrongDemTime != 0) {
//							lstWrongDemTimes.add((int)(nextRec.getTimestamp() - lastWrongDemTime));
//						}
//						lastWrongDemTime = nextRec.getTimestamp();
//					}
//					view = viewEvt.view;
//					leader = view % N;
//					nbViewChange++;
//					isSelectLChange = false;
//				}
//
//				// If algorithm is trying to recover
//				if(computeTimeRecov) {
//					if(viewEvt.view > expectedView) {
//						expectedView = viewEvt.view ;
//						recoveredProc.clear();
//					}
//
//					if(viewEvt.view == expectedView) {
//						// If crash not already detected then compute first suspicion time
//						if(!crashDetected) {
//							crashDetected = true;
//							firstSuspicionTime = nextRec.getTimestamp();
//							lstFirstSuspicionTimes.add((int)(firstSuspicionTime - timeOfCrash));
//						}
//
//						// Add the process to the set of the recovered ones.
//						recoveredProc.add(nextRec.getProc());
//
//						if(recoveredProc.containsAll(aliveProc)) {
//							if(aliveProc.contains(expectedView % N)) {
//								// If all alive process recovered And new designated process is alive then
//								// crash recovery sucessfully terminate.
//								lstReelectionTimes.add((int)(nextRec.getTimestamp() - firstSuspicionTime));
//								lstRecoveryTimes.add((int)(nextRec.getTimestamp() - timeOfCrash));
//								computeTimeRecov = false;
//
//								goodPeriod = true;
//								startTimeGoodPeriod = nextRec.getTimestamp();
//							} else {
//								// If all alive process recovered but new designated process isn't alive
//								// wait the recovery of an alive process.
//								expectedView++;
//								recoveredProc.clear();
//							}
//						} else {
//							tmpTimeOfRecovery = nextRec.getTimestamp();
//						}
//					}
//				} else {
//					if(!goodPeriod && viewEvt.view == view) {
//						// Update the set of up to date processes
//						procUpToDate.add(nextRec.getProc());
//						if(procUpToDate.containsAll(aliveProc)) {
//							// If all processes are up to date a good period is just starting
//							goodPeriod = true;
//							startTimeGoodPeriod = nextRec.getTimestamp();
//						}
//					}
//				}
//				break;
//
//			case BetterLeader:
//				BetterLeader betterEvt = (BetterLeader) nextRec;
//				nbBetterLeader++;
//				isSelectLChange = true;
//				if(nextRec.getProc() < 3 || betterEvt.newLeader > 2) {
//					// Write rtts vector in a file when a useless or wrong better election occurs.
//					String currLogLine = nextRec.getLogLine();
//					int tagIdx = currLogLine.indexOf("RTTs");
//					if(tagIdx != -1) {
//						tagIdx = currLogLine.indexOf("[", tagIdx);
//						String [] rttVectors = currLogLine.substring(tagIdx).split(";");
//						rttVecWriter.println("------------------------------");
//						if(nextRec.getProc() < 3) {
//							nbBetterUseless++;
//							rttVecWriter.print("Useless ");
//						} else if(betterEvt.newLeader > 2) {
//							nbBetterWrong++;
//							rttVecWriter.print("Wrong ");
//						}
//						rttVecWriter.println("better leader selected: " + nextRec.getProc() + "->" + betterEvt.newLeader);
//						rttVecWriter.println("Alive proc: " + aliveProc.toString());
//
//						for(int i = 0; i < rttVectors.length; i++) {
//							rttVecWriter.println(rttVectors[i]);
//						}
//					}
//				}
//				lstBetterLeaderEvent.add(new Pair(nextRec.getProc(), betterEvt.newLeader));
//				break;
//
//			case Suspect:
//				break;
//
//			case TimeReply:
//				TimeReply trEvt = (TimeReply)nextRec; 
////				lstTimeToReplies.add(trEvt.view);
//				break;
//
//			case RTT:
//				RTT rttEvt = (RTT) nextRec;
//				// Infinite value for MajRTT are not interesting
//				if(rttEvt.majRtt != Double.MAX_VALUE) {
//					lstMajRtt.add(rttEvt.majRtt);
//				}
//
//			default:
//				break;
//			}
//		}
//
//		rttVecWriter.flush();
//		rttVecWriter.close();
//
//		// Print in file the sorted Logs
//		try {
//			PrintWriter printWriter = new PrintWriter("sortedLog.out");
//			for(LogRecord nextRec: logList) {
//				if(nextRec.getType() != LogType.RTT) {
//					printWriter.println(nextRec.getLogLine());
//				}
//			}
//			printWriter.flush();
//			printWriter.close();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//			
//		
//		// Print the average and std-dev for each metrics
//		Pair<Double, Double> meanAndStdDev = listProcess(lstRecoveryTimes, "recoveryTimes.out") ;
//		System.out.println("Recovery Times avg: " + meanAndStdDev.getKey());
//		System.out.println("Recovery Times stdDev: " + meanAndStdDev.getValue());
//		System.out.println("-----------------------");
//
//		meanAndStdDev = listProcess(lstFirstSuspicionTimes, "firstSuspTimes.out");
//		System.out.println("1st Susp Times avg: " + meanAndStdDev.getKey());
//		System.out.println("1st Susp Times stdDev: " + meanAndStdDev.getValue());
//		System.out.println("-----------------------");
//
//		meanAndStdDev = listProcess(lstReelectionTimes, "reelectionTimes.out") ;
//		System.out.println("Reelection Times avg: " + meanAndStdDev.getKey());
//		System.out.println("Reelection Times stdDev: " + meanAndStdDev.getValue());
//		System.out.println("-----------------------");
//
//		meanAndStdDev = listProcess(lstWrongDemTimes, "wrongDemTimes.out") ;
//		System.out.println("Wrong dem times avg: " + meanAndStdDev.getKey());
//		System.out.println("Wrong dem times stdDev: " + meanAndStdDev.getValue());
//		System.out.println("-----------------------");
//
//		meanAndStdDev = listProcess(lstGoodPeriodDurations, "goodPerDurations.out") ;
//		System.out.println("Good per duration avg: " + meanAndStdDev.getKey());
//		System.out.println("Good per duration stdDev: " + meanAndStdDev.getValue());
//		System.out.println("-----------------------");
//
//		meanAndStdDev = listProcess(lstMajRtt, "majRtts.out") ;
//		System.out.println("Maj rtt avg: " + meanAndStdDev.getKey());
//		System.out.println("Maj rtt stdDev: " + meanAndStdDev.getValue());
//		System.out.println("-----------------------");
//
//		meanAndStdDev = listProcess(lstTimeToReplies, "timesToReplies.out") ;
//		System.out.println("Time To reply avg: " + meanAndStdDev.getKey());
//		System.out.println("Time To reply stdDev: " + meanAndStdDev.getValue());
//		System.out.println("-----------------------");
//
//		int availableLeaderTime = 0;
//		for(Integer goodPerDur: lstGoodPeriodDurations) {
//			availableLeaderTime += goodPerDur;
//		}	
//		int i = logList.size();
//		do {
//			i--;
//		} while (logList.get(i).getType() != LogType.RTT && logList.get(i).getType() != LogType.TimeReply);
//		double experienceDuration = logList.get(i).getTimestamp() - logList.get(0).getTimestamp();
//		System.out.println("Leader Availability: " + (((double)availableLeaderTime/experienceDuration)*100));
//		System.out.println("-----------------------");
//
//		System.out.println("Nb of view change: " + nbViewChange);
//		System.out.println("Nb of leader crash: " + nbLeaderCrash+ " or " + lstRecoveryTimes.size());
//		System.out.println("Nb of better leader: " + nbBetterLeader);
//
//		System.out.println("    which is useless: " + nbBetterUseless);
//		System.out.println("    which is wrong: " + nbBetterWrong);
//
//		System.out.println("Nb of wrong suspect: " + lstWrongDemTimes.size()); 
//	}

	/** Print the list in a file and return mean and std dev*/
	private Pair<Double, Double> listProcess(List<? extends Number> lst, String filename) {
		double mean = 0;
		double stdDev = 0;
		
		try {
			PrintWriter printWriter = new PrintWriter(new File(dir, filename));
			for(Number currInt: lst) {
				mean += currInt.doubleValue();
				printWriter.println(currInt);
			}
			mean = mean / lst.size();

			for(Number currInt: lst) {
				stdDev += Math.pow(mean-currInt.doubleValue(), 2);
			}
			stdDev /= lst.size();
			stdDev = Math.sqrt(stdDev);

			printWriter.flush();
			printWriter.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new Pair<Double, Double>(mean, stdDev);
	}

	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws ParseException, IOException {
		if(args.length < 1) {
			usage();
			System.exit(1);
		}

		Pair<Integer, BufferedReader>[] logFiles = new Pair[args.length];

		for(int i = 0; i < args.length; i++) {
			try {
				System.out.println(args[i]);
//				String name = args[i].split("\\.")[0];
				
				// Assumes a format of replica__n, where n is 1-based replica number
				String name = args[i].split("__")[1].substring(0, 1);
				// The benchmark framework designed by the polish students
				// uses 1-based replica numbers. We have to convert to 0-based.
				int id = Integer.parseInt(name) - 1;
				logFiles[i] = new Pair(
						id, new BufferedReader(new FileReader(args[i])));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		LogAnalyzer logAnalyze = new LogAnalyzer(logFiles);
		logAnalyze.parseLogs();
		logAnalyze.writeLogs();
//		for (LogRecord log : logAnalyze.logList) {
//			System.out.println(log);
//		}
		
		Analyzer analyzer = new Analyzer(logAnalyze.logList, logAnalyze.N);
		analyzer.analyze();
	}

	private static void usage() {
		System.out.println(
				"Invalid arguments. Usage:\n" + 
		"   java lsr.leader.LogAnalyzer logfiles");
	}
}

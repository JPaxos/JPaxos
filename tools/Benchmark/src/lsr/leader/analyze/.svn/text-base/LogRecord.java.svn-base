package lsr.leader.analyze;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * Store the content of a log line
 * 
 * @author Donzé Benjamin
 * @author Nuno Santos (LSR)
 */
public class LogRecord implements Comparable<LogRecord> {
	private static final SimpleDateFormat sdf = new SimpleDateFormat("kk:mm:ss.SSS");
	private static final Pattern splitter = Pattern.compile("[\\s,\\[\\]]+");
	
	// Process number
	public final int p;
	// Type of the log
	public final LogType type;
	// Contain the whole log line as a String
	public final String logLine;
	private long timestamp;

	public enum LogType {
		Start, 		// Process started or recovered
		Stop,  		// Process crashed or terminated
		View,  		// Change view = change leader
		BetterLeader, // Selected a better leader
		Suspect,  	// Suspect a process
		RTT,      	// RTT measurement
		TimeReply   // Paxos time reply
	}; 
		

	public static LogRecord parse(String logLine, int procID) {
		String tokens[] = splitter.split(logLine);		
//		System.out.println("Parsing: " + logLine);
//		System.out.println("Tokens: " + Arrays.toString(tokens));
		long timestamp;
		try {
			timestamp = sdf.parse(tokens[0]).getTime();
		} catch (ParseException e) {
			return null;
		}
			
		if(logLine.contains("Leader oracle starting")) {
			return new Start(logLine, procID, tokens, timestamp);

		} else if(logLine.contains("Leader oracle stopping")) {
			return new Stop(logLine, procID, tokens, timestamp);

		} else if (logLine.contains("New view")) {
			return new NewView(logLine, procID, tokens, timestamp);

		} else if(logLine.contains("Select better leader")) {
			return new BetterLeader(logLine, procID, tokens, timestamp);

		} else if(logLine.contains("Suspecting leader")) {
			return new Suspect(logLine, procID, tokens, timestamp);

		} else if(logLine.contains("Time to reply")) {
			return new TimeReply(logLine, procID, tokens, timestamp);

		} else if(logLine.contains("New RTT vector")) {
			return new RTT(logLine, procID, tokens, timestamp);

		} else {
			return null;
		}
	}
	
	/**
	 * 
	 * @param logLine
	 * @param procID The process id corresponding to this log
	 * @throws ParseException
	 */
	protected LogRecord(LogType type, String logLine, int procID, long timestamp) {
		this.type = type;
		this.logLine = logLine;
		this.p = procID;
		this.timestamp = timestamp;
	}

	public int getProc() {
		return p;
	}

	public LogType getType() {
		return type;
	}

	public String getLogLine() {
		return logLine;
	}

	@Override
	public String toString() {	
		return getTimestamp() + " [p" + p + ", " + type + "]";
	}
	
	@Override
	public int compareTo(LogRecord o) {
		long thisVal = this.getTimestamp();
		long anotherVal = o.getTimestamp();
		return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
	}

	public void subtract(long runStart) {
		this.timestamp -= runStart;		
	}

	public long getTimestamp() {
		return timestamp;
	}
}

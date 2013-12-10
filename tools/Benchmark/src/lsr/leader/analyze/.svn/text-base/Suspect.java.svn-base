package lsr.leader.analyze;


public class Suspect extends LogRecord {
	
	public final int suspectedProc;
	
	public Suspect(String logLine, int procID, String[] tokens, long timestamp) {
		super(LogType.Suspect, logLine, procID, timestamp);		
		this.suspectedProc = Integer.parseInt(tokens[5]);
	}

	@Override
	public String toString() {
		return super.toString() + ", suspected process " + suspectedProc;
	}
}

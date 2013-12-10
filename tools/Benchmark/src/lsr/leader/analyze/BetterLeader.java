package lsr.leader.analyze;


public class BetterLeader extends LogRecord {

	public final int newLeader;

	public BetterLeader(String logLine, int procID, String[] tokens,
			long timestamp) {
		super(LogType.BetterLeader, logLine, procID, timestamp);
		
		newLeader = Integer.parseInt(tokens[6]);
	}
	
	@Override
	public String toString() {
		return super.toString() + ", New leader: " + newLeader;
	}

}

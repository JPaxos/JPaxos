package lsr.leader.analyze;


public class NewView extends LogRecord {

	public final int view;
	public final int leader;

	public NewView(String logLine, int procID, String[] tokens, long timestamp) {
		super(LogType.View, logLine, procID, timestamp);
		
		view = Integer.parseInt(tokens[5]);
		leader = Integer.parseInt(tokens[7]);
	}
	
	@Override
	public String toString() {
		return super.toString() + " view=" + view + ", leader=" + leader;
	}

}

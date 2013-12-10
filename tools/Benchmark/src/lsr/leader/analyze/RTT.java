package lsr.leader.analyze;

import lsr.common.Util;

public class RTT extends LogRecord {

	public final double[] rttVector;
	public final double majRtt; 

	public RTT(String logLine, int procID, String[] tokens, long timestamp) {
		super(LogType.RTT, logLine, procID, timestamp);

		// If ever I run experiments with more than 32 processes, 
		// this array is not big enough
		double[] tmp = new double[32];
		int i = 0;
		while (!tokens[i+6].startsWith("Maj")) {			
			tmp[i] = Double.parseDouble(tokens[i+6]);
			i++;
		}
		
		this.rttVector = new double[i];
		System.arraycopy(tmp, 0, rttVector, 0, i);
		
		this.majRtt = Double.parseDouble(tokens[i+7]); 
	}
	
	@Override
	public String toString() {
		return super.toString() + " " + Util.toString(rttVector) + ", Maj: " + Util.toString(majRtt);
	}
}

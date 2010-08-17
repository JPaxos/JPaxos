package lsr.paxos.statistics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import lsr.common.RequestId;


public class ClientStats {
	
	private RequestId lastReqSent = null;
	private long lastReqStart = -1;
	private final Writer pw;
	
	/** Whether the previous request got a busy answer. */
	private int busyCount = 0;
	private int redirectCount = 0;
	private int timeoutCount = 0;
	
//	/** Singleton */
//	private static ClientStats instance;	
//	public static ClientStats initialize(long cid) throws IOException {
//		assert instance == null : "Already initialized";		
//		instance = new ClientStats(cid);
//		return instance;
//	}
//	
//	public static ClientStats getInstance() {
//		return instance;
//	}
	
	
	public ClientStats(long cid) throws IOException {
		pw = new FileWriter("client-"+cid+".stats.log");
		pw.write("% seqNum\tSent\tDuration\tRedirect\tBusy\tTimeout\n");
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
//					synchronized (lock) {
						pw.flush();
//					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}

	public void requestSent(RequestId reqId) {
		if (this.lastReqSent != null) {			
			if (!(isRetransmit() || this.lastReqSent.equals(reqId))) {
				throw new AssertionError("Multiple requests sent. Prev: " + this.lastReqSent +", current:"+ reqId);
			}
//			System.out.println("Retransmission: " + req);
		} else {
			this.lastReqSent = reqId;
			this.lastReqStart = System.nanoTime();			
		}
	}
	
	private boolean isRetransmit() {
		return (busyCount + redirectCount + timeoutCount) > 0;
	}

	public void replyRedirect() {
		redirectCount++;		
	}
	
	public void replyBusy() {		
		busyCount++;		
	}
	
	public void replyOk(RequestId reqId) throws IOException {
		int duration = (int) (System.nanoTime() - lastReqStart);
		pw.write(reqId.getSeqNumber() + 
				"\t" + lastReqStart/1000 +
				"\t" + duration/1000 + 
				"\t" + redirectCount +
				"\t" + busyCount +
				"\t" + timeoutCount + "\n");
		
		lastReqSent = null;
		lastReqStart = -1;
		busyCount = 0;
		busyCount = 0;
		redirectCount = 0;
	}
	
	public void replyTimeout() {		
		timeoutCount++;		
	}

}

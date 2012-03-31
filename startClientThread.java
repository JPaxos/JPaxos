import java.io.*;
import java.lang.Runtime;
import java.lang.Runnable;
import java.util.logging.Logger;
import java.util.Random;
import java.util.Arrays;
import java.util.logging.Level;

/*import lsr.paxos.*;
import lsr.paxos.client.*;
import lsr.common.*;*/
import lsr.paxos.test.EchoClient;


public class startClientThread implements Runnable {
	
    private EchoClient client;
	
    public startClientThread() {
		client = new EchoClient(1024);
    }
	
    public void run(){
		try {
			client.run();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
    }
	
	/*
	private static final int DEFAULT_RETRASMIT_TIME = 2;
	private static final int MAX_RETRASMIT_TIME = 30;
	
	private final int reqSize = 1024;
	private final Random r = new Random();
	private final int ansize = 1024;
	private Client client = null;
	
	private byte[] request = null;
	
	public startClientThread() {
		try {
			this.client = new Client(new Configuration());
			this.request = new byte[reqSize];
			Arrays.fill(request, (byte)66);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		client.connect();
		try {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {}
			_logger.info("Start sending requests. Size: " + reqSize);
			
			int retransmitTime = DEFAULT_RETRASMIT_TIME;
			long duration = 0;
			while (true) {
				long start = System.currentTimeMillis();
				byte[] reply = null;
				do {
					try {
						reply = client.execute(request);
						duration += System.currentTimeMillis() - start;
						retransmitTime = DEFAULT_RETRASMIT_TIME;
						// Check validity of answer
						if (reply.length != ansize) {
							throw new AssertionError("Unexpected answer. " +
							                         "Expected size: " + ansize + ", " +
							                         "Answer size: " + reply.length);
						}
					} catch (ReplicationException ex) {
						_logger.info("System busy. Retransmitting in " + retransmitTime + " (" + ex.getMessage() + ")");
						try {
							Thread.sleep(retransmitTime);
						} catch (InterruptedException e) {}
						retransmitTime= Math.min(retransmitTime*2, MAX_RETRASMIT_TIME);
					}
				} while (reply == null);
			}
		} catch (Throwable t) {
			_logger.log(Level.SEVERE, "Thread dying", t);
			t.printStackTrace();
		}
	}
	 
	public static String toHexString(byte[] block) {
		StringBuffer buf = new StringBuffer();
		char[] hexChars = { 
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
			'A', 'B', 'C', 'D', 'E', 'F' };
		int len = block.length;
		int high = 0;
		int low = 0;
		for (int i = 0; i < len; i++) {
			high = ((block[i] & 0xf0) >> 4);
			low = (block[i] & 0x0f);
			buf.append(hexChars[high]);
			buf.append(hexChars[low]);
		} 
		return buf.toString();
	}
	
	private final static Logger _logger = Logger.getLogger(startClientThread.class.getCanonicalName());*/
}

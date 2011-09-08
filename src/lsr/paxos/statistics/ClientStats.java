package lsr.paxos.statistics;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import lsr.common.RequestId;

public interface ClientStats {
    public abstract void requestSent(RequestId reqId);

    public abstract void replyRedirect();

    public abstract void replyBusy();

    public abstract void replyOk(RequestId reqId) throws IOException;

    public abstract void replyTimeout();

    /**
     * Empty implementation
     */
    public final class ClientStatsNull implements ClientStats {
        public void requestSent(RequestId reqId) {
        }

        public void replyRedirect() {
        }

        public void replyBusy() {
        }

        public void replyOk(RequestId reqId) {
        }

        public void replyTimeout() {
        }
    }

    /**
     * Full implementation
     */
    public final class ClientStatsImpl implements ClientStats {

        private RequestId lastReqSent = null;
        private long lastReqStart = -1;

        /** Whether the previous request got a busy answer. */
        private int busyCount = 0;
        private int redirectCount = 0;
        private int timeoutCount = 0;
        
        
        final static class LoggerBuffer extends Thread {
            public final PerformanceLogger pLogger; 
            public final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(2048);
            
            public LoggerBuffer() {
                pLogger = PerformanceLogger.getLogger("client");                
                pLogger.log("% cid\tseqNum\tSent\tDuration\tRedirect\tTimeout\n");
                
                // Prioritize writing to the log file
                setPriority(MAX_PRIORITY);
            }
            
            public void log(String str) {
                queue.add(str);
            }
            
            @Override
            public void run() {
                while (true) {
                    try {
                        String str = queue.take();
                        pLogger.log(str);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
        
        private static LoggerBuffer logBuffer;
        private static long simStart;
        static {            
            simStart = System.nanoTime();
            logBuffer = new LoggerBuffer();
            logBuffer.start();            
        }

        public ClientStatsImpl()  {}

        public void requestSent(RequestId reqId) {
            if (this.lastReqSent != null) {
                if (!(isRetransmit() || this.lastReqSent.equals(reqId))) {
                    throw new AssertionError("Multiple requests sent. Prev: " + this.lastReqSent +
                                             ", current:" + reqId);
                }
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
            // milliseconds
            int sendRelativeTime = (int)((lastReqStart - simStart)/1000/1000);  
            double duration = ((double)(System.nanoTime() - lastReqStart))/1000/1000;
            String str = String.format("%4d %5d %5d %6.2f %1d %1d\n", 
                    reqId.getClientId(), reqId.getSeqNumber(), 
                    sendRelativeTime, duration,
                    redirectCount, timeoutCount);
            logBuffer.log(str);
            lastReqSent = null;
            lastReqStart = -1;
            busyCount = 0;
            timeoutCount = 0;
            redirectCount = 0;
        }

        public void replyTimeout() {
            timeoutCount++;
        }
    }
}

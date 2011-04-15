package lsr.paxos.statistics;

import java.io.IOException;

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
        public void requestSent(RequestId reqId) {}
        public void replyRedirect() {}
        public void replyBusy() {}
        public void replyOk(RequestId reqId) {}
        public void replyTimeout() {}
    }

    /**
     * Full implementation
     */
    public final class ClientStatsImpl implements ClientStats {

        private RequestId lastReqSent = null;
        private long lastReqStart = -1;
        private final PerformanceLogger pLogger;

        /** Whether the previous request got a busy answer. */
        private int busyCount = 0;
        private int redirectCount = 0;
        private int timeoutCount = 0;

        public ClientStatsImpl(long cid) throws IOException {
            this.pLogger = PerformanceLogger.getLogger("client-" + cid);
            pLogger.log("% seqNum\tSent\tDuration\tRedirect\tBusy\tTimeout\n");            
        }

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
            long duration = System.nanoTime() - lastReqStart;
            pLogger.log(reqId.getSeqNumber() + "\t" + lastReqStart / 1000 + "\t" + duration / 1000 +
                     "\t" + redirectCount + "\t" + busyCount + "\t" + timeoutCount + "\n");

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

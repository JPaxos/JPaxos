package lsr.paxos.statistics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

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
    public class ClientStatsNull implements ClientStats {
        @Override
        public void requestSent(RequestId reqId) {
        }

        @Override
        public void replyRedirect() {
        }

        @Override
        public void replyBusy() {
        }

        @Override
        public void replyOk(RequestId reqId) {
        }

        @Override
        public void replyTimeout() {
        }
    }

    /**
     * Full implementation
     */
    public class ClientStatsImpl implements ClientStats {

        private RequestId lastReqSent = null;
        private long lastReqStart = -1;
        private final Writer pw;

        /** Whether the previous request got a busy answer. */
        private int busyCount = 0;
        private int redirectCount = 0;
        private int timeoutCount = 0;

        // /** Singleton */
        // private static ClientStats instance;
        // public static ClientStats initialize(long cid) throws IOException {
        // assert instance == null : "Already initialized";
        // instance = new ClientStats(cid);
        // return instance;
        // }
        //
        // public static ClientStats getInstance() {
        // return instance;
        // }

        public ClientStatsImpl(long cid) throws IOException {
            pw = new FileWriter("client-" + cid + ".stats.log");
            pw.write("% seqNum\tSent\tDuration\tRedirect\tBusy\tTimeout\n");
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        pw.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        @Override
        public void requestSent(RequestId reqId) {
            if (this.lastReqSent != null) {
                if (!(isRetransmit() || this.lastReqSent.equals(reqId))) {
                    throw new AssertionError("Multiple requests sent. Prev: " + this.lastReqSent +
                                             ", current:" + reqId);
                }
                // System.out.println("Retransmission: " + req);
            } else {
                this.lastReqSent = reqId;
                this.lastReqStart = System.nanoTime();
            }
        }

        private boolean isRetransmit() {
            return (busyCount + redirectCount + timeoutCount) > 0;
        }

        @Override
        public void replyRedirect() {
            redirectCount++;
        }

        @Override
        public void replyBusy() {
            busyCount++;
        }

        @Override
        public void replyOk(RequestId reqId) throws IOException {
            long duration = System.nanoTime() - lastReqStart;
            pw.write(reqId.getSeqNumber() + "\t" + lastReqStart / 1000 + "\t" + duration / 1000 +
                     "\t" + redirectCount + "\t" + busyCount + "\t" + timeoutCount + "\n");

            lastReqSent = null;
            lastReqStart = -1;
            busyCount = 0;
            timeoutCount = 0;
            redirectCount = 0;
        }

        @Override
        public void replyTimeout() {
            timeoutCount++;
        }

    }
}

package lsr.paxos.statistics;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import lsr.common.RequestId;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

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

        private long firstRequestTS = -1;
        private long lastRequestTS = -1;

        /** Whether the previous request got a busy answer. */
        private int busyCount = 0;
        private int redirectCount = 0;
        private int timeoutCount = 0;
        private final long clientId;

        private final SummaryStatistics stats = new SummaryStatistics();

        private final static Set<ClientStatsImpl> clients = new HashSet<ClientStatsImpl>();

        private final static TimeSeries2 timeSeriesStats = new TimeSeries2();

        final static class LoggerBuffer extends Thread {
            public final PerformanceLogger pLogger;
            public final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(
                    10 * 1024);

            public LoggerBuffer() {
                pLogger = PerformanceLogger.getLogger("client");
                pLogger.log("% cid\tseqNum\tSent\tDuration\tRedirect\tTimeout\n");

                // Ensure that everything is written to the log
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        // Stop the logging thread, giving it a chance to finish
                        // writing the logs to disk
                        this.interrupt();
                        try {
                            this.join();
                        } catch (InterruptedException e) {
                        }
                        pLogger.flush();
                    }
                });
                this.setPriority(MAX_PRIORITY);
            }

            public void log(String str) {
                // if (!queue.offer(str)) {
                // failed.incrementAndGet();
                // }
                try {
                    queue.put(str);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            @Override
            public void run() {
                try {
                    // Main loop, normal operation
                    while (true) {
                        String str = queue.take();
                        pLogger.log(str);
                    }
                } catch (InterruptedException e) {
                    // Shutdown
                    try {
                        while (!queue.isEmpty()) {
                            pLogger.log(queue.take());
                        }
                    } catch (InterruptedException e1) {
                    }
                    return;
                }
            }
        }

        // private static LoggerBuffer logBuffer;
        private static long simStart;
        static {
            simStart = System.nanoTime();
            // Must create the PerformanceLogger before the VM starts the
            // shutdown sequence
            // Otherwise, the static constructor of PerformanceLogger tries to
            // register a
            // shutdown hook, which if done during shutdown results in an
            // exception.
            final PerformanceLogger p = PerformanceLogger.getLogger("exec-count");

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    // int total = 0;
                    p.log("% cid #requests thrpt mean stddev  min  max\n");
                    for (ClientStatsImpl c : clients) {
                        SummaryStatistics stats = c.stats;
                        // Convert duration from nanoseconds to seconds.
                        double duration = (c.lastRequestTS - c.firstRequestTS) / 1000.0 / 1000 / 1000;
                        double thrpt = stats.getN() / duration;

                        // total += stats.getN();
                        String str = String.format("%4d %5d %6.2f %6.2f %6.2f %6.2f %6.2f\n",
                                c.clientId, stats.getN(), thrpt, stats.getMean(),
                                stats.getStandardDeviation(), stats.getMin(), stats.getMax());
                        p.log(str);
                    }
                    // p.logln("-1 " + total);
                    p.flush();
                }
            });

            timeSeriesStats.start();
            // logBuffer = new LoggerBuffer();
            // logBuffer.start();
        }

        public ClientStatsImpl(long cid) {
            this.clientId = cid;
            synchronized (clients) {
                clients.add(this);
            }
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

        // private static final FieldPosition fpos = new FieldPosition(0);
        // private static final ThreadLocal<DecimalFormat> tLocal = new
        // ThreadLocal<DecimalFormat>() {
        // protected DecimalFormat initialValue() {
        // return new DecimalFormat("#0.00"); // this will helps you to always
        // keeps in two decimal places
        // };
        // };

        public void replyOk(RequestId reqId) throws IOException {
            // milliseconds
            long now = System.nanoTime();
            // now - simStart is in nanoseconds. Wait for 30 milliseconds after
            // start
            if ((now - simStart) > 30 * 1000 * 1000) {
                if (firstRequestTS == -1) {
                    firstRequestTS = now;
                }
                lastRequestTS = now;
                double duration = ((double) (now - lastReqStart)) / 1000 / 1000;
                stats.addValue(duration);
                timeSeriesStats.addValue(duration);
                // logBuffer.log(Double.toString(duration) +"\n");
            }

            // int sendRelativeTime = (int)((lastReqStart -
            // simStart)/1000/1000);
            // StringBuffer sb = new StringBuffer(48);
            // sb.append(reqId.getClientId()).append("\t");
            // sb.append(reqId.getSeqNumber()).append("\t");
            // sb.append(sendRelativeTime).append("\t");
            // tLocal.get().format(duration, sb, fpos); sb.append("\n");
            // logBuffer.log(sb.toString());

            lastReqSent = null;
            lastReqStart = -1;
            busyCount = 0;
            timeoutCount = 0;
            redirectCount = 0;
        }

        public void replyTimeout() {
            timeoutCount++;
        }

        private final static Logger logger =
                Logger.getLogger(ClientStats.class.getCanonicalName());
    }
}

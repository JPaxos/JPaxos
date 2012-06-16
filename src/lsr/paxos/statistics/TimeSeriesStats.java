package lsr.paxos.statistics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

final public class TimeSeriesStats {

    private final int BIN_SIZE = 100;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // System time
    private long startTime;

    // Relative to startTime
    private int prev;

    private final SummaryStatistics stats = new SummaryStatistics();
    private final Object lock = new Object();
    private final PerformanceLogger pLogger = PerformanceLogger.getLogger("client-timeseries");

    public TimeSeriesStats() {
        pLogger.log("% time  #requests thrpt  mean  stddev  min  max\n");
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
        this.prev = 0;

        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    flushStats();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        },
                BIN_SIZE, BIN_SIZE, TimeUnit.MILLISECONDS);
    }

    public void addValue(double time) {
        synchronized (lock) {
            stats.addValue(time);
        }
    }

    private void flushStats() {
        int now = (int) (System.currentTimeMillis() - startTime);

        long n;
        double max, min, mean, stddev;
        synchronized (lock) {
            n = stats.getN();
            max = stats.getMax();
            min = stats.getMin();
            mean = stats.getMean();
            stddev = stats.getStandardDeviation();
            stats.clear();
        }
        // Normalize to throughput by second
        double thrpt = (n / (now - prev)) * (1000 / BIN_SIZE);
        if (n == 0) {
            max = 0;
            min = 0;
            mean = 0;
            stddev = 0;
        }

        String str = String.format("%6d %5d %5.0f %7.2f %7.2f %7.2f %7.2f\n",
                now, n, thrpt, mean, stddev, min, max);
        pLogger.log(str);
        pLogger.flush();
        prev = now;
    }

}

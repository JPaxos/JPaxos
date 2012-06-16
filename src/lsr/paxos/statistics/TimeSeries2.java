package lsr.paxos.statistics;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

final public class TimeSeries2 {

    // In seconds
    private final int MAX_RUN_DURATION = 200;
    // In milliseconds
    private final int BIN_SIZE = 100;

    // System time
    private long startTime;

    private final SummaryStatistics[] stats = new SummaryStatistics[MAX_RUN_DURATION * 1000 /
                                                                    BIN_SIZE];
    private final PerformanceLogger pLogger = PerformanceLogger.getLogger("client-timeseries");

    public TimeSeries2() {
        pLogger.log("% time  #requests thrpt  mean  stddev  min  max\n");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                flushStats();
            }
        });
    }

    public void start() {
        // Preallocate to save time during the run
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new SummaryStatistics();
        }
        this.startTime = System.currentTimeMillis();
    }

    /**
     * 
     * @param responseTime in seconds
     */
    public void addValue(double responseTime) {
        int now = getRelativeTime();
        int reqSentTime = now - (int) Math.round(responseTime);
        int bin = reqSentTime / BIN_SIZE;
        // Lock only the bin. Contention with all the requests that are placed
        // in the same bin.
        synchronized (stats[bin]) {
            stats[bin].addValue(responseTime);
        }
    }

    private int getRelativeTime() {
        return (int) (System.currentTimeMillis() - startTime);
    }

    private void flushStats() {
        int top = stats.length - 1;
        // Find the last slot with non-zero count
        while (top >= 0 && stats[top].getN() == 0)
            top--;

        for (int i = 0; i <= top; i++) {
            int time = i * BIN_SIZE;
            long n = stats[i].getN();
            double max, min, mean, stddev;
            if (n == 0) {
                max = 0;
                min = 0;
                mean = 0;
                stddev = 0;
            } else {
                max = stats[i].getMax();
                min = stats[i].getMin();
                mean = stats[i].getMean();
                stddev = stats[i].getStandardDeviation();
            }
            // Normalize to throughput by second
            double thrpt = n * (1000.0 / BIN_SIZE);
            String str = String.format("%6d %5d %5.0f %7.2f %7.2f %7.2f %7.2f\n",
                    time, n, thrpt, mean, stddev, min, max);
            pLogger.log(str);
        }
        pLogger.flush();
    }

}

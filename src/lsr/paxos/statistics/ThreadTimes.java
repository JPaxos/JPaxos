package lsr.paxos.statistics;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import lsr.common.ProcessDescriptor;

/**
 * Collects the wall-clock, CPU and user time for all the running threads and
 * saves this information periodically to the file
 * "replica-<localid>-threadTimes"
 * 
 * @author Nuno Santos (LSR)
 */
public class ThreadTimes {
    private static ThreadTimes instance;

    public static void initialize() {
        if (ProcessDescriptor.getInstance().benchmarkRunReplica) {
            instance = new ThreadTimesImpl();
        } else {
            instance = new ThreadTimes();
        }
    }

    public static ThreadTimes getInstance() {
        return instance;
    }

    public void startInstance(int cid) {
    }
}

final class ThreadTimesImpl extends ThreadTimes {
    private final ThreadMXBean bean;
    private final RuntimeMXBean rtbean;

    private long[] tids;
    private long[] cpuStart;
    private long[] userStart;
    // Uptime of JVM when we started collecting statistics (init called)
    private long uptimeStart;

    // Delay initialization until first call to startInstance, to ensure
    // that all application threads are created at that point.
    private boolean initialized = false;
    private int lastCid;

    private final PerformanceLogger pLogger;
//    private final PerformanceLogger contentionLogger;
    private long lastLogTs = 0;

    /** Create a polling thread to track times. */
    ThreadTimesImpl() {
        bean = ManagementFactory.getThreadMXBean();
        rtbean = ManagementFactory.getRuntimeMXBean();
        int localid = ProcessDescriptor.getInstance().localId;
        pLogger = PerformanceLogger.getLogger("replica-" + localid + "-threadTimes");
//        contentionLogger = PerformanceLogger.getLogger("replica-" + localid + "-contention");
        // No shutdown hook for finalizing the log. Some threads are already dead when 
        // the shutdown hook is called, and therefore are not included in the report, 
        // so we underreport the time.

       
//        contentionLogger.log("ObjectMonitorUsage: " +  bean.isObjectMonitorUsageSupported() + "\n");
//        contentionLogger.log("ThreadContentionMonitoring: " + bean.isThreadContentionMonitoringSupported()+"\n");
//        contentionLogger.log("SynchronizerUsageSupported: " + bean.isSynchronizerUsageSupported()+"\n");
//        bean.setThreadContentionMonitoringEnabled(true);        
    }

    public void startInstance(int cid) {
        // Do not log the first 10 seconds. JVM warmup
        long uptime = rtbean.getUptime(); 
        if (uptime < 10 * 1000) {
            return;
        }
        if (!initialized) {
            init();
        }
        this.lastCid = cid;
//        // Do not write a log every instance, for averaging over time
//        // it's enough to log less often
        if (cid % 64 == 0) {
            doLog();
        }
        // Log every second
//        if (uptime - lastLogTs  > 1000) {
//            lastLogTs = uptime;
//            doLog();
//            doContentionLog();
//        }
    }

    private void doLog() {
        long userTime = 0;
        long cpuTime = 0;
//        StringBuilder sb = new StringBuilder(tids.length*16);
        for (int i = 0; i < tids.length; i++) {
            long id = tids[i];
            // Reduce to milliseconds, which is enough for the available
            // precision
            long cDelta = (bean.getThreadCpuTime(id) - cpuStart[i]) / 1000000;
            long uDelta = (bean.getThreadUserTime(id) - userStart[i]) / 1000000;
//            sb.append(id).append(" ").append(cDelta).append(' ').append(uDelta).append("\t");
            userTime += uDelta;
            cpuTime += cDelta;
        }
        int testTime = (int) (rtbean.getUptime() - uptimeStart);
//        pLogger.log(lastCid + "\t" + testTime + "\t" + cpuTime + "\t" + userTime + "\t " + sb.toString() + "\n");        
        pLogger.log(lastCid + "\t" + testTime + "\t" + userTime + "\t" + cpuTime + "\n");
    }
    
//    private void doContentionLog(){
//        StringBuilder sb = new StringBuilder("\nUptime: " + rtbean.getUptime()+"\n");
//        Formatter f = new Formatter(sb);
//        for (int i = 0; i < tids.length; i++) {
//            ThreadInfo ti = bean.getThreadInfo(tids[i]);
//            if (ti == null) {
//                sb.append(tids[i] + " missing\n");
//            } else { 
//                f.format("%18s (%4d %4d) (%6d %6d) ", ti.getThreadName(), 
//                        ti.getBlockedCount(), ti.getBlockedTime(), 
//                        ti.getWaitedCount(), ti.getWaitedTime());
//                sb.append(ti.getThreadState());
//                if (ti.getLockName() != null) {
//                    sb.append(" on " + ti.getLockName());
//                }
//                sb.append(" ");
//
//                if (ti.isSuspended()) {
//                    sb.append(" (suspended)");
//                }
//                if (ti.isInNative()) {
//                    sb.append(" (in native)");
//                }            
//                sb.append("\n");
//            }
//        }
//        contentionLogger.log(sb.toString());
//    }

    private void init() {
        // Usually enabled by default, but set it anyway.
        bean.setThreadCpuTimeEnabled(true);

        final long[] ids = bean.getAllThreadIds();
        int pos = 0;
        StringBuilder sb = new StringBuilder(256);
        sb.append("% cid\trealTime\tuser\tcpu\t");
        for (int i = 0; i < ids.length; i++) {
            ThreadInfo tinfo = bean.getThreadInfo(ids[i]);
            String name = tinfo.getThreadName();
            if (name.equals("Signal Dispatcher") ||
                    name.equals("Finalizer") ||
                    name.equals("Reference Handler") ||
                    name.equals("DestroyJavaVM")) {
                continue;
            }
            sb.append(ids[i]).append(":").append(tinfo.getThreadName()).append(" ");
            ids[pos] = ids[i];
            pos++;
        }
        pLogger.log(sb.toString()+"\n");

        this.tids = new long[pos];
        System.arraycopy(ids, 0, tids, 0, pos);
        this.cpuStart = new long[pos];
        this.userStart = new long[pos];

        for (int i = 0; i < tids.length; i++) {
            cpuStart[i] = bean.getThreadCpuTime(tids[i]);
            userStart[i] = bean.getThreadUserTime(tids[i]);
        }
        uptimeStart = rtbean.getUptime();
        initialized = true;
        doLog();
        pLogger.flush();
    }
}
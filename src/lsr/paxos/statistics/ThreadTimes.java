package lsr.paxos.statistics;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;

/**
 * Collects the wall-clock, CPU and user time for all the running threads and
 * saves this information periodically to the file "replica-<localid>-threadTimes"
 *  
 * @author Nuno Santos (LSR)
 */
public class ThreadTimes {
    private static ThreadTimes instance;
    public static void initialize() {
        if (ProcessDescriptor.getInstance().benchmarkRun) {
            instance = new ThreadTimesImpl();
        } else {
            instance = new ThreadTimes();
        }
    }
    public static ThreadTimes getInstance() {
        return instance;
    }
    public void startInstance(int cid) {}    
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
    
    /** Create a polling thread to track times. */
    ThreadTimesImpl() {
        bean = ManagementFactory.getThreadMXBean( );
        rtbean = ManagementFactory.getRuntimeMXBean();
        int localid = ProcessDescriptor.getInstance().localId;
        pLogger = PerformanceLogger.getLogger("replica-"+localid+"-threadTimes");
        // Useless. Some threads are already dead when the shutdown hook is called,
        // and therefore are not included in the report, so we underreport the time.
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                pLogger.flush();
//            }
//        });
    }

    public void startInstance(int cid) {
        // Do not log the first 10 seconds. JVM warmup
        if (rtbean.getUptime() < 10*1000) {
            return;
        }
        if (!initialized ) {
            init();
        }
//        this.cid = cid;
//
//        for (int i = 0; i < tids.length; i++) {
//            long id = tids[i];
//            cpuStart[i] = bean.getThreadCpuTime(id);
//            userStart[i] = bean.getThreadUserTime(id);
//            if ( cpuStart[i] == -1 || userStart[i] == -1 ) {
//                ThreadInfo tinfo = bean.getThreadInfo(id);
//                logger.warning("Thread died: " + tinfo.getThreadName());
//                continue;
//            }
//        }
        this.lastCid = cid;
        // Do not write a log every instance, for averaging over time 
        // it's enough to log less often
        if (cid % 16 == 0) {
            doLog();
        }
    }
    
    private void doLog() {
        long userTime=0;
        long cpuTime=0;
//        StringBuilder sb = new StringBuilder(tids.length*16);
//        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < tids.length; i++) {
            long id = tids[i];
            // Reduce to milliseconds, which is enough for the available precision 
            long cDelta = (bean.getThreadCpuTime(id) - cpuStart[i])/1000000;
            long uDelta = (bean.getThreadUserTime(id) - userStart[i])/1000000;
//            sb.append(id).append(" ").append(cDelta).append(' ').append(uDelta).append("\t");
            userTime += uDelta;
            cpuTime += cDelta;
        }
        int testTime = (int) (rtbean.getUptime()-uptimeStart); 
//        pLogger.log(lastCid + "\t" + userTime + "\t" + cpuTime + "\t " + sb.toString() + "\n");
        pLogger.log(lastCid + "\t" + testTime +"\t" + userTime + "\t" + cpuTime + "\n");
    }

    private void init() {
        // Usually enabled by default, but set it anyway.
        bean.setThreadCpuTimeEnabled(true);
        
        final long[] ids = bean.getAllThreadIds( );
        int pos = 0;
        StringBuilder sb = new StringBuilder(256);        
        sb.append("% cid\trealTime\tuser\tcpu\t(threadid cpu user)*: ");
        for (int i = 0; i < ids.length; i++) {            
            ThreadInfo tinfo = bean.getThreadInfo(ids[i]);
//            logger.info(tinfo.toString());
            String name = tinfo.getThreadName();
            if (name.equals("Signal Dispatcher") ||
                    name.equals("Finalizer") ||
                    name.equals("Reference Handler") ||
                    name.equals("DestroyJavaVM")) 
            {
                continue;
            }
            sb.append(ids[i] + ":" + tinfo.getThreadName() + " ");
            ids[pos] = ids[i];
            pos++;
        }
        pLogger.log(sb.toString());

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

    private final static Logger logger = Logger.getLogger(ThreadTimes.class.getCanonicalName());
}
package lsr.paxos.statistics;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;

public class ReplicaStats {
    /** Singleton */
    private static ReplicaStats instance;

    public static ReplicaStats initialize(int n, int localID) throws IOException {
        // assert instance == null : "Already initialized";
        if (ProcessDescriptor.getInstance().benchmarkRunReplica) {
            instance = new ReplicaStatsFull(n, localID);
        } else {
            instance = new ReplicaStats();
        }
        return instance;
    }

    public static ReplicaStats getInstance() {
        return instance;
    }

    /* Stub implementation. For non-benchmark runs */
    public void consensusStart(int cid, int size, int k, int alpha) {
    }

    public void retransmit(int cid) {
    }

    public void consensusEnd(int cid) {
    }

    public void advanceView(int newView) {
    }

    public void setRequestsInInstance(int cid, int requestsInInstance) {
    }
}

/*
 * Full implementation.
 */
final class ReplicaStatsFull extends ReplicaStats {

    static final class Instance implements Comparable<Instance> {
        public final static long RUN_START=System.nanoTime();
        
        public final long start;
        public final int cid;
        /** Number of requests ordered on this instance/batch */
        public final int nBatches;
        public final int valueSize;
        /** Number of instances active at the time this instance was started */
        public final int alpha;
        
        
        public long end;
        public int retransmit = 0;
        public int requestsInInstance = 0;

        public Instance(int cid, long firstStart, int valueSize, int nBatches, int alpha) {
            this.cid = cid;
            this.start = firstStart;
            this.nBatches = nBatches;
            this.valueSize = valueSize;
            this.alpha = alpha;
        }

        public int compareTo(Instance o) {
            long thisVal = this.start;
            long anotherVal = o.start;
            return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Instance)) {
                return false;
            }
            Instance o = (Instance) obj;
            return this.cid == o.cid;
        }

        @Override
        public int hashCode() {
            return cid;
        }

        public long getDuration() {
            return end - start;
        }

        public static String getHeader() {
            return "Start\tDuration\t#Batches\t#Reqs\tSize\tAlpha";
        }
        

        
//        public String toString() {
//            int  instStartTime = (int) ((start-RUN_START) / 1000 / 1000);
//            double duration = getDuration()/1000.0/1000.0;
//            
//            StringBuffer sb = new StringBuffer(64);
//            sb.append(instStartTime).append("\t");
//            f.format(duration, sb, fpos); sb.append("\t");
//            sb.append(nRequests).append("\t");
//            sb.append(requestsInInstance).append("\t");
//            sb.append(valueSize).append("\t");
//            sb.append(alpha);
//            return sb.toString();
//        }
    }

    private final int n;
    private final int localID;
    private final PerformanceLogger pLogger;
    private final Map<Integer, Instance> instances = 
        Collections.synchronizedMap(new HashMap<Integer, Instance>());

    // Current view of each process
    private int view = -1;
    boolean firstLog = true;

    ReplicaStatsFull(int n, int localID) throws IOException {
        this.n = n;
        this.localID = localID;
        pLogger = PerformanceLogger.getLogger("replica-" + localID);
        pLogger.log("% Consensus\t" + Instance.getHeader() + "\n");
    }

    public void consensusStart(int cid, int size, int k, int alpha) {
        assert !instances.containsKey(cid) : "Instance not null: " + instances.get(cid);
        Instance cInstance = new Instance(cid, System.nanoTime(), size, k, alpha);
        instances.put(cid, cInstance);
    }

    public void retransmit(int cid) {
        Instance instance = instances.get(cid);
        if (instance != null) {
            instance.retransmit++;
        }
    }

    public void consensusEnd(int cid) {
        Instance cInstance = instances.get(cid);
        if (cInstance == null) {
            // Can occur in view change if this process is the leader that
            // decides
            // an instance that was started by a previous leader.
            // Ignore this instance, as it is not possible to accurately measure
            // the instance
            // time using clocks from two processes.
            return;
        }

        cInstance.end = System.nanoTime();
    }
    
    /** Must be called after consensusEnd. */
    public void setRequestsInInstance(int cid, int requestsInInstance) {
        // Ignore log if process is not leader.
        if (!isLeader()) {
            return;
        }
        Instance instance = instances.remove(cid);        
        if (instance != null) {
            instance.requestsInInstance = requestsInInstance;
            // Write to log
            writeInstance(cid, instance);        
        } else {
            // This happens during view change. The new leader may decide some instances
            // based on the contents of the PrepareOK message. Therefore, it has no
            // record on its statistics of having started this instance 
            // We just ignore the instance. It might have been logged in the process that proposed it.            
            if (logger.isLoggable(Level.INFO)) 
                logger.info("No entry for instance: " + cid);
        }
    }
    
    public void advanceView(int newView) {
        this.view = newView;
        for (Integer cid : instances.keySet()) {
            Instance cInstance = instances.get(cid);
            cInstance.end = -1; // Indicates that the process started but did not finish the instance.
            writeInstance(cid, cInstance);
        }
        instances.clear();
    }
    
    
    
    static final class DecimalFormatData {
        public final FieldPosition fpos = new FieldPosition(0);
        public final DecimalFormat df = new DecimalFormat("#0.00");
    }
    
    private static final ThreadLocal<DecimalFormatData> tLocal = new ThreadLocal<DecimalFormatData>() {
        protected DecimalFormatData initialValue() {
            return new DecimalFormatData();  // this will helps you to always keeps in two decimal places
        };
    };
    
    
    private void writeInstance(int cId, Instance cInstance) {
        int  instStartTime = (int) ((cInstance.start-Instance.RUN_START) / 1000 / 1000);
        // -1 indicates that this started but did not decided the instance. View change.
        double duration = cInstance.end == -1 ? -1.0 : cInstance.getDuration()/1000.0/1000.0;

        StringBuffer sb = new StringBuffer(40);
        sb.append(cId).append("\t");
        sb.append(instStartTime).append("\t");
        DecimalFormatData tl = tLocal.get();
        tl.df.format(duration, sb, tl.fpos); sb.append("\t");
        sb.append(cInstance.nBatches).append("\t");
        sb.append(cInstance.requestsInInstance).append("\t");
        // valueSize = nBatches*8+4. Each batch takes 8 bytes (repID:seqNumber)
        sb.append(cInstance.valueSize).append("\t");
        sb.append(cInstance.alpha).append("\n");
        
        pLogger.log(sb.toString());
    }

    /**
     * True if the process considers itself the leader
     * 
     * @return
     */
    private boolean isLeader() {
        return view % n == localID;
    }
    private final static Logger logger = Logger.getLogger(ReplicaStats.class.getCanonicalName());

}

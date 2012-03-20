package lsr.paxos.replica;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Using local system clock generates ID's.
 * 
 * If a process starts, gives IDs, crashes, and recovers in less than system
 * clock resolution (usually 16 ms), it'll start with already given ID's.
 * 
 * As this is barely possible (if even possible), we assume it's a stable,
 * correct ID generator.
 * 
 * Please notice, the system clock may not be drastically changed during
 * operation!
 */
public class TimeBasedIdGenerator implements IdGenerator {

    private final AtomicLong clientId;
    private final int replicaCount;

    /**
     * Creates new generator. Should be created only once during a program runs.
     * 
     * @param localId - ID of replica
     * @param replicaCount - number of replicas
     */
    public TimeBasedIdGenerator(int localId, int replicaCount) {
        if (replicaCount < 1 || localId < 0 || localId >= replicaCount) {
            throw new IllegalArgumentException();
        }
        this.replicaCount = replicaCount;
        long initialId = System.currentTimeMillis() * 1000 * replicaCount;
        initialId -= initialId % replicaCount;
        initialId += localId;
        this.clientId = new AtomicLong(initialId);
    }

    public long next() {
        // clientId += replicaCount;
        // return clientId;
        return clientId.addAndGet(replicaCount);
    }

}

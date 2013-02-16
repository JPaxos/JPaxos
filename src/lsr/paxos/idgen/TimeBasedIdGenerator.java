package lsr.paxos.idgen;

import static lsr.common.ProcessDescriptor.processDescriptor;

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
    private final int replicaCount = processDescriptor.numReplicas;

    /**
     * Creates new generator. Should be created only once during a program runs.
     * 
     * @param localId - ID of replica
     * @param replicaCount - number of replicas
     */
    public TimeBasedIdGenerator() {
        long initialId = System.currentTimeMillis() * 1000 * replicaCount;
        initialId -= initialId % replicaCount;
        initialId += processDescriptor.localId;
        this.clientId = new AtomicLong(initialId);
    }

    public long next() {
        return clientId.addAndGet(replicaCount);
    }

}

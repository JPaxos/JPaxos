package lsr.paxos;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class NamedThreadFactory implements ThreadFactory {
    private final String name;
    private final AtomicInteger sequencer = new AtomicInteger(1);
    private boolean useSeqNumber;

    public NamedThreadFactory(String name) {
        this(name, true);
    }

    /** 
     * @param name 
     * @param useSeqNumber Whether to add a sequence number to threads created by this factory
     */
    public NamedThreadFactory(String name, boolean useSeqNumber) {
        this.name = name;
        this.useSeqNumber = useSeqNumber;
    }
    
    public Thread newThread(Runnable r) {
        String threadName = name;
        if (useSeqNumber) {
            threadName += "-" + sequencer.getAndIncrement();
        }
        return new Thread(r, threadName);
    }
}

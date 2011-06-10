package lsr.paxos;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class NamedThreadFactory implements ThreadFactory {
    private final String name;
    private final AtomicInteger sequencer = new AtomicInteger(1);

    public NamedThreadFactory(String name) {
        this.name = name;
    }

    public Thread newThread(Runnable r) {
        return new Thread(r, name + "-" + sequencer.getAndIncrement());
    }
}

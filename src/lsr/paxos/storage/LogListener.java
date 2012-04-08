package lsr.paxos.storage;

import java.util.EventListener;

/**
 * Is informed about every log size change. Size change doesn't have to be
 * increased or decreased by one.
 */
public interface LogListener extends EventListener{

    /** Called when size changes */
    void logSizeChanged(int newsize);
}

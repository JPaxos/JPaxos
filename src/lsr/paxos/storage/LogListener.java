package lsr.paxos.storage;

/**
 * Is informed about every log size change. Size change doesn't have to be
 * increased or decreased by one.
 */
public interface LogListener {

    /** Called when size changes */
    void logSizeChanged(int newsize);
}

package lsr.paxos;

/**
 * Is informed about every log size change. Size change doesn't have to be
 * increased or decreased by one.
 */

interface LogListener {

    /** Called when size changes */
    void logSizeChanged(int newsize);

}

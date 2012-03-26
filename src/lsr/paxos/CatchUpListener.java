package lsr.paxos;

/**
 * Represents a class that wants to be informed as the catch-up achieves a
 * certain state.
 * 
 * @author JK
 */
public interface CatchUpListener {

    /**
     * Informs that the catch-up decided to stop, i.e. it assumed that the
     * replica is up to date.
     * 
     * Relies on CatchUp.checkCatchupSucceded()
     */
    void catchUpSucceeded();
}

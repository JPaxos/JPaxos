package lsr.common;

public interface DispatcherListener {
    /**
     * Called when there are no more incoming messages waiting to be processed.
     * Objects can register for this event if there are task they want to do
     * only when the system is idle, like retransmitting potentially lost
     * messages.
     */
    public void onIncomingQueueEmpty();

}

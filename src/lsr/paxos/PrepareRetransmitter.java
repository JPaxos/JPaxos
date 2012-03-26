package lsr.paxos;

import java.util.BitSet;

import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;

/**
 * Represents the retransmitter of <code>Prepare</code> message used by
 * proposer. This retransmitter decided to which processes the
 * <code>Prepare</code> message should be sent based on <code>PrepareOk</code>
 * responses from other processes.
 */
interface PrepareRetransmitter {
    /**
     * Starts transmitting specified <code>Prepare</code> message to given
     * acceptors processes.
     * 
     * @param prepare - the prepare message to retransmit
     * @param acceptor - the set of processes to send <code>Prepare</code>
     *            message to
     */
    void startTransmitting(Prepare prepare, BitSet acceptor);

    /**
     * Stops retransmitting of <code>Prepare</code> message.
     */
    void stop();

    /**
     * Updates list of processes to sent <code>Prepare</code> message based on
     * the <code>PrepareOk</code> response.
     * 
     * @param message - the <code>PrepareOk</code> response
     * @param sender - the id of process which sent the response
     */
    void update(PrepareOK message, int sender);

    /**
     * Returns true if the majority of processes had responded with
     * <code>PrepareOk</code>.
     * 
     * @return true if the majority had responded; false otherwise
     */
    boolean isMajority();
}
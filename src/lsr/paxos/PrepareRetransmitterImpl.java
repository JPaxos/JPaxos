package lsr.paxos;

import java.util.BitSet;

import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;

/**
 * Simple implementation of <code>PrepareRetransmitter</code> interface. It is
 * using the <code>Retransmitter</code> class and retransmits only to processes
 * that <code>PrepareOk</code> response has not been received.
 */
class PrepareRetransmitterImpl implements PrepareRetransmitter {
    private final Retransmitter retransmitter;
    private RetransmittedMessage prepareRetransmitter;
    private BitSet prepared = new BitSet();

    public PrepareRetransmitterImpl(Retransmitter retransmitter) {
        this.retransmitter = retransmitter;
    }

    public void startTransmitting(Prepare prepare, BitSet acceptor) {
        prepared.clear();
        prepareRetransmitter = retransmitter.startTransmitting(prepare, acceptor);
    }

    public void stop() {
        prepareRetransmitter.stop();
    }

    public void update(PrepareOK message, int sender) {
        prepared.set(sender);
        prepareRetransmitter.stop(sender);
    }

    public boolean isMajority() {
        return prepared.cardinality() > ProcessDescriptor.getInstance().numReplicas / 2;
    }
}
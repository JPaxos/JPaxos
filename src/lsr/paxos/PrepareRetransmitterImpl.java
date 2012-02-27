package lsr.paxos;

import java.util.BitSet;

import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;

/**
 * Simple implementation of <code>PrepareRetransmitter</code> interface. It is
 * using the <code>ActiveRetransmitter</code> class and retransmits only to processes
 * that <code>PrepareOk</code> response has not been received.
 */
final class PrepareRetransmitterImpl implements PrepareRetransmitter {
    private final ActiveRetransmitter retransmitter;
    private RetransmittedMessage prepareRetransmitter;
    private BitSet prepared = new BitSet();

    public PrepareRetransmitterImpl(ActiveRetransmitter retransmitter) {
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
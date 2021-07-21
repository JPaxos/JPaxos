package lsr.paxos;

import java.util.BitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;

/**
 * Simple implementation of <code>PrepareRetransmitter</code> interface. It is
 * using the <code>ActiveRetransmitter</code> class and retransmits only to
 * processes that <code>PrepareOk</code> response has not been received.
 */
public final class PrepareRetransmitterImpl implements PrepareRetransmitter {
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
        if(prepareRetransmitter!=null)
            prepareRetransmitter.stop();
        else
            logger.info("Stopping retransmitter with no active message");
    }

    public void update(PrepareOK message, int sender) {
        prepared.set(sender);
        prepareRetransmitter.stop(sender);
    }

    public boolean isMajority() {
        return prepared.cardinality() > ProcessDescriptor.processDescriptor.numReplicas / 2;
    }

    private final static Logger logger = LoggerFactory.getLogger(PrepareRetransmitterImpl.class);
}
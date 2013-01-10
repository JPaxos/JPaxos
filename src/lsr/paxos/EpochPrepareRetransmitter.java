package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;

import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.storage.Storage;

public class EpochPrepareRetransmitter implements PrepareRetransmitter {
    private final ActiveRetransmitter retransmitter;
    private RetransmittedMessage prepareRetransmitter;
    // keeps epochs of received prepareOk messages.
    private long[] prepareEpoch;
    private BitSet prepared = new BitSet();
    private final Storage storage;
    private final int numReplicas;

    public EpochPrepareRetransmitter(ActiveRetransmitter retransmitter, Storage storage) {
        this.retransmitter = retransmitter;
        this.storage = storage;
        numReplicas = processDescriptor.numReplicas;
        prepareEpoch = new long[numReplicas];
    }

    public void startTransmitting(Prepare prepare, BitSet acceptor) {
        for (int i = 0; i < numReplicas; i++) {
            prepareEpoch[i] = -1;
        }
        prepared.clear();
        prepareRetransmitter = retransmitter.startTransmitting(prepare, acceptor);
    }

    public void stop() {
        prepareRetransmitter.stop();
    }

    public void update(PrepareOK message, int sender) {
        if (sender == processDescriptor.localId) {
            prepareEpoch[sender]=storage.getEpoch()[sender];
            prepared.set(sender);
            return;
        }

        // update storage - storage has greatest seen epochs.
        storage.updateEpoch(message.getEpoch());

        // Mark that we got prepareOk; overwrite received epoch only if we got
        // now newer
        prepareEpoch[sender] = Math.max(prepareEpoch[sender], message.getEpoch()[sender]);

        for (int i = 0; i < numReplicas; i++) {
            // Here, if we detected stale message, we discard it and reduce
            // prepared set
            if (prepareEpoch[i] == storage.getEpoch()[i]) {
                stop(i);
            } else {
                start(i);
            }
        }
    }

    public boolean isMajority() {
        return prepared.cardinality() >= processDescriptor.majority;
    }

    private void stop(int i) {
        if (!prepared.get(i)) {
            prepared.set(i);
            prepareRetransmitter.stop(i);
        }
    }

    private void start(int i) {
        if (prepared.get(i)) {
            prepared.clear(i);
            prepareRetransmitter.start(i);
        }
    }
}
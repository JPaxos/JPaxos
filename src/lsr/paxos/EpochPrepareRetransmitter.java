package lsr.paxos;

import java.util.BitSet;

import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.storage.Storage;

class EpochPrepareRetransmitter implements PrepareRetransmitter {
    private final ActiveRetransmitter retransmitter;
    private RetransmittedMessage prepareRetransmitter;
    private long[] prepareEpoch;
    private BitSet prepared;
    private final Storage storage;
    private final int numReplicas;

    public EpochPrepareRetransmitter(ActiveRetransmitter retransmitter, Storage storage) {
        this.retransmitter = retransmitter;
        this.storage = storage;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        prepareEpoch = new long[numReplicas];
        prepared = new BitSet();
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
        storage.updateEpoch(message.getEpoch());
        prepareEpoch[sender] = Math.max(prepareEpoch[sender], message.getEpoch()[sender]);

        for (int i = 0; i < numReplicas; i++) {
            if (prepareEpoch[i] == storage.getEpoch()[i]) {
                stop(i);
            } else {
                start(i);
            }
        }
    }

    public boolean isMajority() {
        return prepared.cardinality() > numReplicas / 2;
    }

    private void stop(int i) {
        prepared.set(i);
        prepareRetransmitter.stop(i);
    }

    private void start(int i) {
        prepared.clear(i);
        prepareRetransmitter.start(i);
    }
}
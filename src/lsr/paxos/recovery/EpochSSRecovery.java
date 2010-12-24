package lsr.paxos.recovery;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.BitSet;

import lsr.common.ProcessDescriptor;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.PaxosImpl;
import lsr.paxos.RetransmittedMessage;
import lsr.paxos.Retransmitter;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

public class EpochSSRecovery extends RecoveryAlgorithm {
    private Storage storage;
    private Paxos paxos;
    private RecoveryAnswerListener recoveryAnswerListener;
    private final SnapshotProvider snapshotProvider;
    private final DecideCallback decideCallback;
    private final File epochFile;
    private RetransmittedMessage recoveryRetransmitter;

    public EpochSSRecovery(SnapshotProvider snapshotProvider, DecideCallback decideCallback,
                           String logPath) {
        this.snapshotProvider = snapshotProvider;
        this.decideCallback = decideCallback;
        this.epochFile = new File(logPath, "sync.epoch");
    }

    public void start() throws IOException {
        storage = createStorage();
        paxos = createPaxos(decideCallback, snapshotProvider, storage);

        // do not execute recovery mechanism on first run
        long localEpochNumber = storage.getEpoch()[ProcessDescriptor.getInstance().localId];
        if (localEpochNumber == 1) {
            fireRecoveryListener(paxos, null);
            return;
        }

        // send recovery message to all
        Retransmitter retransmitter = new Retransmitter(paxos.getNetwork(),
                ProcessDescriptor.getInstance().numReplicas, paxos.getDispatcher());
        Recovery recoveryMessage = new Recovery(localEpochNumber);
        recoveryRetransmitter = retransmitter.startTransmitting(recoveryMessage);

        recoveryAnswerListener = new RecoveryAnswerListener();
        Network.addMessageListener(MessageType.RecoveryAnswer, recoveryAnswerListener);
    }

    protected Paxos createPaxos(DecideCallback decideCallback, SnapshotProvider snapshotProvider,
                                Storage storage) throws IOException {
        return new PaxosImpl(decideCallback, snapshotProvider, storage);
    }

    private Storage createStorage() throws IOException {
        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();

        Storage storage = new InMemoryStorage();
        if (storage.getView() % descriptor.numReplicas == descriptor.localId)
            storage.setView(storage.getView() + 1);

        long[] epoch = new long[descriptor.numReplicas];
        epoch[descriptor.localId] = readEpoch() + 1;
        writeEpoch(epoch[descriptor.localId]);

        storage.setEpoch(epoch);

        return storage;
    }

    private long readEpoch() throws IOException {
        if (!epochFile.exists())
            return 0;
        DataInputStream stream = new DataInputStream(new FileInputStream(epochFile));
        long localEpochNumber = stream.readLong();
        stream.close();
        return localEpochNumber;
    }

    private void writeEpoch(long localEpoch) throws IOException {
        // TODO TZ - epoch numbers should be appended to the file, not truncated
        DataOutputStream stream = new DataOutputStream(new FileOutputStream(epochFile, false));
        stream.writeLong(localEpoch);
        stream.close();
    }

    // Download all instances before <code>nextId</code>
    private void startCatchup(long nextId) {
        // TODO TZ - start catchup
        // paxos.getCatchup().start();

        // if storage.getFirstUncommitted() >= nextId then finish
        // wait until receiving all instances to nextId
        onCatchupFinished();
    }

    private void onCatchupFinished() {
        fireRecoveryListener(paxos, null);
    }

    private class RecoveryAnswerListener implements MessageHandler {
        private BitSet received;
        private RecoveryAnswer answerFromLeader = null;

        public RecoveryAnswerListener() {
            received = new BitSet(ProcessDescriptor.getInstance().numReplicas);
        }

        public void onMessageReceived(Message msg, int sender) {
            assert msg.getType() == MessageType.RecoveryAnswer;
            RecoveryAnswer recoveryAnswer = (RecoveryAnswer) msg;
            assert recoveryAnswer.getEpoch().length == storage.getEpoch().length;

            // update epoch vector
            storage.updateEpoch(recoveryAnswer.getEpoch());

            // update view
            if (storage.getView() < recoveryAnswer.getView()) {
                received.clear();
                answerFromLeader = null;
                storage.setView(recoveryAnswer.getView());
            }

            received.set(sender);
            if (recoveryAnswer.getView() % ProcessDescriptor.getInstance().numReplicas == sender) {
                answerFromLeader = recoveryAnswer;
            }

            if (received.cardinality() > ProcessDescriptor.getInstance().numReplicas / 2) {
                startCatchup(answerFromLeader.getNextId());
                Network.removeMessageListener(MessageType.RecoveryAnswer, recoveryAnswerListener);
                recoveryRetransmitter.stop();
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
        }
    }
}

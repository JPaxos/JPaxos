package lsr.paxos.recovery;

import java.io.IOException;
import java.util.BitSet;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.ActiveRetransmitter;
import lsr.paxos.Paxos;
import lsr.paxos.Paxos;
import lsr.paxos.RetransmittedMessage;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.SingleNumberWriter;
import lsr.paxos.storage.Storage;

public class EpochSSRecovery extends RecoveryAlgorithm implements Runnable {
    private static final String EPOCH_FILE_NAME = "sync.epoch";

    private Storage storage;
    private Paxos paxos;
    private RetransmittedMessage recoveryRetransmitter;
    private ActiveRetransmitter retransmitter;
    private SingleThreadDispatcher dispatcher;
    private SingleNumberWriter epochFile;

    private long localEpochNumber;

    private int localId;
    private int numReplicas;

    public EpochSSRecovery(SnapshotProvider snapshotProvider, String logPath)
            throws IOException {
        epochFile = new SingleNumberWriter(logPath, EPOCH_FILE_NAME);
        localId = ProcessDescriptor.getInstance().localId;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        storage = createStorage();
        paxos = createPaxos(snapshotProvider, storage);
        dispatcher = paxos.getDispatcher();
    }

    protected Paxos createPaxos(SnapshotProvider snapshotProvider,
                                Storage storage) throws IOException {
        return new Paxos(snapshotProvider, storage);
    }

    public void start() {
        dispatcher.submit(this);
    }

    public void run() {
        // do not execute recovery mechanism on first run
        localEpochNumber = storage.getEpoch()[localId];
        if (localEpochNumber == 1) {
            onRecoveryFinished();
            return;
        }

        retransmitter = new ActiveRetransmitter(paxos.getNetwork());
        logger.info("Sending recovery message");
        Network.addMessageListener(MessageType.RecoveryAnswer, new RecoveryAnswerListener());
        recoveryRetransmitter = retransmitter.startTransmitting(new Recovery(-1, localEpochNumber));
    }

    private Storage createStorage() throws IOException {
        Storage storage = new InMemoryStorage();
        if (storage.getView() % numReplicas == localId) {
            storage.setView(storage.getView() + 1);
        }

        long[] epoch = new long[numReplicas];
        epoch[localId] = epochFile.readNumber() + 1;
        epochFile.writeNumber(epoch[localId]);

        storage.setEpoch(epoch);

        return storage;
    }

    // Get all instances before <code>nextId</code>
    private void startCatchup(final int nextId) {
        new RecoveryCatchUp(paxos.getCatchup(), storage).recover(nextId, new Runnable() {
            public void run() {
                onRecoveryFinished();
            }
        });
    }

    private void onRecoveryFinished() {
        fireRecoveryListener();
        Network.addMessageListener(MessageType.Recovery, new EpochRecoveryRequestHandler(paxos));
    }

    private class RecoveryAnswerListener implements MessageHandler {
        private BitSet received;
        private RecoveryAnswer answerFromLeader = null;

        public RecoveryAnswerListener() {
            received = new BitSet(numReplicas);
        }

        public void onMessageReceived(Message msg, final int sender) {
            assert msg.getType() == MessageType.RecoveryAnswer;
            final RecoveryAnswer recoveryAnswer = (RecoveryAnswer) msg;
            assert recoveryAnswer.getEpoch().length == storage.getEpoch().length;

            // drop message if came from previous recovery
            if (recoveryAnswer.getEpoch()[localId] != localEpochNumber) {
                return;
            }

            logger.info("Got a recovery answer " + recoveryAnswer +
                        (recoveryAnswer.getView() % numReplicas == sender ? " from leader" : ""));

            dispatcher.submit(new Runnable() {
                public void run() {
                    // update epoch vector
                    storage.updateEpoch(recoveryAnswer.getEpoch());
                    recoveryRetransmitter.stop(sender);
                    received.set(sender);

                    // update view
                    if (storage.getView() < recoveryAnswer.getView()) {
                        storage.setView(recoveryAnswer.getView());
                        answerFromLeader = null;
                    }

                    if (storage.getView() % numReplicas == sender) {
                        answerFromLeader = recoveryAnswer;
                    }

                    if (received.cardinality() > numReplicas / 2) {
                        onCardinality();
                    }
                }
            });
        }

        private void onCardinality() {
            recoveryRetransmitter.stop();
            recoveryRetransmitter = null;

            if (answerFromLeader == null) {
                Recovery recovery = new Recovery(-1, localEpochNumber);
                recoveryRetransmitter = retransmitter.startTransmitting(recovery);
            } else {
                startCatchup((int) answerFromLeader.getNextId());
                Network.removeMessageListener(MessageType.RecoveryAnswer, this);
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
        }
    }

    public Paxos getPaxos() {
        return paxos;
    }

    private static final Logger logger = Logger.getLogger(EpochSSRecovery.class.getCanonicalName());
}

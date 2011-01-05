package lsr.paxos.recovery;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.ProcessDescriptor;
import lsr.paxos.CatchUp;
import lsr.paxos.CatchUpListener;
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

public class EpochSSRecovery extends RecoveryAlgorithm implements Runnable {
    private static final String EPOCH_FILE_NAME = "sync.epoch";

    private Storage storage;
    private Paxos paxos;
    private RetransmittedMessage recoveryRetransmitter;
    private Retransmitter retransmitter;
    private Dispatcher dispatcher;

    private long localEpochNumber;
    private final String logPath;
    private final MessageHandler recoveryRequestHandler;

    private int localId;
    private int numReplicas;

    public EpochSSRecovery(SnapshotProvider snapshotProvider, DecideCallback decideCallback,
                           String logPath, MessageHandler recoveryRequestHandler)
            throws IOException {
        this.logPath = logPath;
        localId = ProcessDescriptor.getInstance().localId;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        storage = createStorage();
        paxos = createPaxos(decideCallback, snapshotProvider, storage);
        dispatcher = paxos.getDispatcher();

        this.recoveryRequestHandler = recoveryRequestHandler;
    }

    protected Paxos createPaxos(DecideCallback decideCallback, SnapshotProvider snapshotProvider,
                                Storage storage) throws IOException {
        return new PaxosImpl(decideCallback, snapshotProvider, storage);
    }

    public void start() {
        dispatcher.dispatch(this);
    }

    public void run() {
        // do not execute recovery mechanism on first run
        localEpochNumber = storage.getEpoch()[localId];
        if (localEpochNumber == 1) {
            onRecoveryFinished();
            return;
        }

        retransmitter = new Retransmitter(paxos.getNetwork(), numReplicas, dispatcher);
        logger.info("Sending recovery message");
        Network.addMessageListener(MessageType.RecoveryAnswer, new RecoveryAnswerListener());
        recoveryRetransmitter = retransmitter.startTransmitting(new Recovery(-1, localEpochNumber));
    }

    private Storage createStorage() throws IOException {
        Storage storage = new InMemoryStorage();
        if (storage.getView() % numReplicas == localId)
            storage.setView(storage.getView() + 1);

        long[] epoch = new long[numReplicas];
        epoch[localId] = readEpoch() + 1;
        writeEpoch(epoch[localId]);

        storage.setEpoch(epoch);

        return storage;
    }

    private long readEpoch() throws IOException {
        File epochFile = new File(logPath, EPOCH_FILE_NAME);
        if (!epochFile.exists())
            return 0;
        DataInputStream stream = new DataInputStream(new FileInputStream(epochFile));
        long localEpochNumber = stream.readLong();
        stream.close();
        return localEpochNumber;
    }

    private void writeEpoch(long localEpoch) throws IOException {
        new File(logPath).mkdirs();
        File tempEpochFile = new File(logPath, EPOCH_FILE_NAME + "_t");
        tempEpochFile.createNewFile();

        DataOutputStream stream = new DataOutputStream(new FileOutputStream(tempEpochFile, false));
        stream.writeLong(localEpoch);
        stream.close();
        if (!tempEpochFile.renameTo(new File(logPath, EPOCH_FILE_NAME)))
            throw new AssertionError("Could not replace the file with epoch number!");
    }

    // Get all instances before <code>nextId</code>
    private void startCatchup(final long nextId) {
        if (nextId == 0) {
            onRecoveryFinished();
            return;
        }

        paxos.getDispatcher().dispatch(new Runnable() {
            public void run() {
                paxos.getStorage().getLog().getInstance((int) nextId - 1);
            }
        });

        CatchUp catchup = paxos.getCatchup();
        catchup.addListener(new InternalCatchUpListener(nextId, catchup));
        catchup.start();
        catchup.startCatchup();
    }

    private void onRecoveryFinished() {
        fireRecoveryListener();
        Network.addMessageListener(MessageType.Recovery, recoveryRequestHandler);
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
            if (recoveryAnswer.getEpoch()[localId] != localEpochNumber)
                return;

            logger.info("Got a recovery answer " + recoveryAnswer +
                        (recoveryAnswer.getView() % numReplicas == sender ? " from leader" : ""));

            dispatcher.dispatch(new Runnable() {
                public void run() {
                    // update epoch vector
                    storage.updateEpoch(recoveryAnswer.getEpoch());
                    recoveryRetransmitter.stop(sender);
                    received.set(sender);

                    // update view
                    if (storage.getView() < recoveryAnswer.getView()) {
                        storage.setView(recoveryAnswer.getView());
                    }

                    if (recoveryAnswer.getView() % numReplicas == sender) {
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
                startCatchup(answerFromLeader.getNextId());
                Network.removeMessageListener(MessageType.RecoveryAnswer, this);
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
        }
    }

    public Paxos getPaxos() {
        return paxos;
    }

    /** Listens for the moment, when recovery can proceed */
    protected class InternalCatchUpListener implements CatchUpListener {

        /** InstanceId up to which all must be known before joining */
        private final long nextId;

        /** The catch-up mechanism - must unregister from it. */
        private final CatchUp catchup;

        public InternalCatchUpListener(long nextId, CatchUp catchup) {
            this.nextId = nextId;
            this.catchup = catchup;
        }

        public void catchUpSucceeded() {
            if (storage.getFirstUncommitted() >= nextId) {
                if (!catchup.removeListener(this))
                    throw new AssertionError("Unable to unregister from catch-up");
                logger.info("Succesfully caught up");
                onRecoveryFinished();
            } else {
                logger.info("Catch up assumed success, but failed to fetch some needed instances");
                catchup.forceCatchup();
            }
        }
    }

    private static final Logger logger = Logger.getLogger(EpochSSRecovery.class.getCanonicalName());
}

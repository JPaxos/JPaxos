package lsr.paxos.recovery;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.NewSingleThreadDispatcher;
import lsr.paxos.ActiveRetransmitter;
import lsr.paxos.RetransmittedMessage;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.core.Paxos;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.replica.Replica;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.SingleNumberWriter;
import lsr.paxos.storage.Storage;

public class EpochSSRecovery extends RecoveryAlgorithm implements Runnable {
    private static final String EPOCH_FILE_NAME = "sync.epoch";

    /*
     * (JK) currently the recovery in parallel with a view change is far from
     * optimal.
     */

    private Storage storage;
    private Paxos paxos;
    private RetransmittedMessage recoveryRetransmitter;
    private ActiveRetransmitter retransmitter;
    private NewSingleThreadDispatcher dispatcher;
    private SingleNumberWriter epochFile;

    private long localEpochNumber;

    private int localId;
    private int numReplicas;

    public EpochSSRecovery(SnapshotProvider snapshotProvider, Replica replica,
                           String logPath)
            throws IOException {
        epochFile = new SingleNumberWriter(logPath, EPOCH_FILE_NAME);
        localId = processDescriptor.localId;
        numReplicas = processDescriptor.numReplicas;
        storage = createStorage();
        paxos = new Paxos(snapshotProvider, storage, replica);
        dispatcher = paxos.getDispatcher();
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

        retransmitter = new ActiveRetransmitter(paxos.getNetwork(), "EpochSSRecoveryRetransmitter");
        retransmitter.init();
        Network.addMessageListener(MessageType.RecoveryAnswer, new RecoveryAnswerListener());
        Recovery recovery = new Recovery(-1, localEpochNumber);
        logger.info(processDescriptor.logMark_Benchmark, "Sending {}", recovery);
        recoveryRetransmitter = retransmitter.startTransmitting(recovery);
        if (logger.isInfoEnabled(processDescriptor.logMark_Benchmark2019))
            logger.info(processDescriptor.logMark_Benchmark2019, "R1 S {}", recovery.getEpoch());
    }

    private Storage createStorage() throws IOException {
        Storage storage = new InMemoryStorage();
        if (processDescriptor.isLocalProcessLeader(storage.getView())) {
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
        fireRecoveryFinished();
        Network.addMessageListener(MessageType.Recovery, new EpochRecoveryRequestHandler(paxos));
    }

    private class RecoveryAnswerListener implements MessageHandler {
        private Map<Integer, long[]> received;
        private RecoveryAnswer answerFromLeader = null;

        public RecoveryAnswerListener() {
            received = new HashMap<Integer, long[]>(numReplicas, 1);
        }

        public void onMessageReceived(Message msg, final int sender) {
            assert msg.getType() == MessageType.RecoveryAnswer;
            final RecoveryAnswer recoveryAnswer = (RecoveryAnswer) msg;
            assert recoveryAnswer.getEpoch().length == storage.getEpoch().length;

            // drop message if came from previous recovery
            if (recoveryAnswer.getEpoch()[localId] != localEpochNumber) {
                return;
            }

            if (logger.isInfoEnabled(processDescriptor.logMark_Benchmark2019))
                logger.info(processDescriptor.logMark_Benchmark2019, "R2 R {} {}",
                        recoveryAnswer.getEpoch()[sender], sender);

            logger.debug(processDescriptor.logMark_Benchmark, "Received {}", msg);

            if (logger.isInfoEnabled())
                logger.info("Got a recovery answer {}{}", recoveryAnswer,
                        (processDescriptor.getLeaderOfView(recoveryAnswer.getView()) == sender
                                ? " from leader" : ""));

            dispatcher.submit(new Runnable() {
                public void run() {
                    if (recoveryRetransmitter == null)
                        return;

                    // update epoch vector
                    storage.updateEpoch(recoveryAnswer.getEpoch());
                    recoveryRetransmitter.stop(sender);
                    received.put(sender, recoveryAnswer.getEpoch());

                    // remove stale answers
                    ArrayList<Integer> toRemove = new ArrayList<Integer>();
                    for (Integer p : received.keySet()) {
                        if (received.get(p)[p] != storage.getEpoch()[p]) {
                            toRemove.add(p);
                        }
                    }
                    for (Integer p : toRemove) {
                        received.remove(p);
                        recoveryRetransmitter.start(p);
                    }

                    // update view
                    if (storage.getView() < recoveryAnswer.getView()) {
                        storage.setView(recoveryAnswer.getView());
                        answerFromLeader = null;
                    }

                    if (processDescriptor.getLeaderOfView(storage.getView()) == sender) {
                        answerFromLeader = recoveryAnswer;
                    }

                    if (received.size() > numReplicas / 2) {
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

    private static final Logger logger = LoggerFactory.getLogger(EpochSSRecovery.class);
}

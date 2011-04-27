package put.consensus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import lsr.common.Configuration;
import lsr.paxos.replica.Replica;
import lsr.service.AbstractService;
import put.consensus.listeners.CommitListener;
import put.consensus.listeners.ConsensusListener;
import put.consensus.listeners.RecoveryListener;

public class SerializablePaxosConsensus extends AbstractService implements CommitableConsensus {

    private Replica replica;
    private ConsensusDelegateProposer client;
    private BlockingQueue<Runnable> operationsToBeDone = new LinkedBlockingQueue<Runnable>();
    private List<ConsensusListener> consensusListeners = new Vector<ConsensusListener>();
    private List<RecoveryListener> recoveryListeners = new Vector<RecoveryListener>();
    private List<CommitListener> commitListeners = new Vector<CommitListener>();
    private int lastDeliveredRequest = -1;

    public SerializablePaxosConsensus(Configuration configuration, int localId)
            throws IOException {
        replica = new Replica(configuration, localId, this);
    }

    public final void start() throws IOException {
        replica.start();

        client = new ConsensusDelegateProposerImpl();

        startThreads();
    }

    private final void startThreads() {
        // Starting thread for all actions
        Thread thread = new Thread() {
            public void run() {
                try {
                    while (true) {
                        operationsToBeDone.take().run();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        thread.start();
    }

    public final byte[] execute(final byte[] value, final int seqNo) {
        operationsToBeDone.add(new Runnable() {
            public void run() {
                Object val = byteArrayToObject(value);
                synchronized (consensusListeners) {
                    for (ConsensusListener l : consensusListeners) {
                        l.decide(val);
                    }
                }
                lastDeliveredRequest = seqNo;
            }
        });
        return new byte[0];
    }

    public final void propose(Object obj) {
        client.propose(obj);
    }

    public final void commit(final Object commitData) {
        operationsToBeDone.add(new Runnable() {
            public void run() {
                for (CommitListener listner : commitListeners) {
                    listner.onCommit(commitData);
                }
                fireSnapshotMade(lastDeliveredRequest, byteArrayFromObject(commitData), null);
            }
        });
    }

    public final void updateToSnapshot(final int instanceId, final byte[] snapshot) {
        operationsToBeDone.add(new Runnable() {
            public void run() {
                lastDeliveredRequest = instanceId;
                for (RecoveryListener listner : recoveryListeners) {
                    listner.recoverFromCommit(byteArrayToObject(snapshot));
                }
            }
        });
    }

    public final void recoveryFinished() {
        super.recoveryFinished();
        operationsToBeDone.add(new Runnable() {
            public void run() {
                for (RecoveryListener listner : recoveryListeners) {
                    listner.recoveryFinished();
                }
            }
        });
    }

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

    public final void addConsensusListener(ConsensusListener listener) {
        synchronized (consensusListeners) {
            consensusListeners.add(listener);
        }
    }

    public final void removeConsensusListener(ConsensusListener listener) {
        synchronized (consensusListeners) {
            consensusListeners.remove(listener);
        }
    }

    public final boolean addCommitListener(CommitListener listener) {
        return commitListeners.add(listener);
    }

    public final boolean removeCommitListener(CommitListener listener) {
        return commitListeners.remove(listener);
    }

    public final boolean addRecoveryListener(RecoveryListener listener) {
        return recoveryListeners.add(listener);
    }

    public final boolean removeRecoveryListener(RecoveryListener listener) {
        return recoveryListeners.remove(listener);
    }

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

    protected Object byteArrayToObject(byte[] bytes) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois;
            ois = new ObjectInputStream(bis);
            return ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected byte[] byteArrayFromObject(Object object) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new ObjectOutputStream(bos).writeObject(object);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public final void askForSnapshot(int lastSnapshotInstance) {
    }

    public final void forceSnapshot(int lastSnapshotInstance) {
    }

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

    public ConsensusDelegateProposer getNewDelegateProposer() throws IOException {
        return new ConsensusDelegateProposerImpl();
    }

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

    public final int getHighestExecuteSeqNo() {
        return lastDeliveredRequest;
    }

}
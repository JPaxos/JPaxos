package put.consensus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import lsr.common.Configuration;
import lsr.paxos.replica.Replica;
import lsr.paxos.replica.Replica.CrashModel;
import lsr.paxos.storage.PublicDiscWriter;
import lsr.paxos.storage.PublicLog;
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

	private PublicDiscWriter discWriter;
	private PublicLog log;

	public SerializablePaxosConsensus(Configuration configuration, int localId) throws IOException {
		replica = new Replica(configuration, localId, this);
		replica.setCrashModel(CrashModel.FullStableStorage);
		replica.setLogPath("consensusLogs/" + localId);
	}

	@Override
	public final void start() throws IOException {
		// These classes should not be here - these are internal PaxosJava
		// classes.
		replica.start();
		log = replica.getPublicLog();
		discWriter = replica.getPublicDiscWriter();

		client = new ConsensusDelegateProposerImpl();

		startThreads();
	}

	private final void startThreads() {
		// Starting thread for all actions
		new Thread() {
			public void run() {
				try {
					while (true)
						operationsToBeDone.take().run();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}.start();
	}

	@Override
	public final byte[] execute(final byte[] value, final int instanceId, final int seqNo) {
		operationsToBeDone.add(new Runnable() {
			@Override
			public void run() {
				Object val = byteArrayToObject(value);
				for (ConsensusListener l : consensusListeners)
					l.decide(val);
				lastDeliveredRequest = seqNo;
			}
		});
		return new byte[0];
	}

	@Override
	public final void propose(Object obj) {
		client.propose(obj);
	}

	@Override
	public final void commit(final Object commitData) {
		operationsToBeDone.add(new Runnable() {
			@Override
			public void run() {
				for (CommitListener listner : commitListeners)
					listner.onCommit(commitData);
				replica.onSnapshotMade(lastDeliveredRequest, byteArrayFromObject(commitData));
			}
		});
	}

	@Override
	public final void updateToSnapshot(final int instanceId, final byte[] snapshot) {
		operationsToBeDone.add(new Runnable() {
			@Override
			public void run() {
				lastDeliveredRequest = instanceId;
				for (RecoveryListener listner : recoveryListeners)
					listner.recoverFromCommit(byteArrayToObject(snapshot));
			}
		});
	}

	@Override
	public final void recoveryFinished() {
		super.recoveryFinished();
		operationsToBeDone.add(new Runnable() {
			@Override
			public void run() {
				for (RecoveryListener listner : recoveryListeners)
					listner.recoveryFinished();
			}
		});
	}

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

	@Override
	public final void addConsensusListener(ConsensusListener listener) {
		consensusListeners.add(listener);
	}

	@Override
	public final void removeConsensusListener(ConsensusListener listener) {
		consensusListeners.remove(listener);
	}

	@Override
	public final boolean addCommitListener(CommitListener listener) {
		return commitListeners.add(listener);
	}

	@Override
	public final boolean removeCommitListener(CommitListener listener) {
		return commitListeners.remove(listener);
	}

	@Override
	public final boolean addRecoveryListener(RecoveryListener listener) {
		return recoveryListeners.add(listener);
	}

	@Override
	public final boolean removeRecoveryListener(RecoveryListener listener) {
		return recoveryListeners.remove(listener);
	}

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

	@Override
	public final void log(Serializable key, Serializable value) throws StorageException {
		discWriter.record(key, value);

	}

	@Override
	public final Object retrieve(Serializable key) throws StorageException {
		return discWriter.retrive(key);
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

	@Override
	public final void askForSnapshot(int lastSnapshotInstance) {
	}

	@Override
	public final void forceSnapshot(int lastSnapshotInstance) {
	}

	@Override
	public final void instanceExecuted(int instanceId) {
	}

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

	@Override
	public ConsensusDelegateProposer getNewDelegateProposer() throws IOException {
		return new ConsensusDelegateProposerImpl();
	}

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

	@Override
	public final int getHighestExecuteSeqNo() {
		return log.getHighestExecuteSeqNo();
	}

	@Override
	public final Object getRequest(int requestNo) {
		byte[] request = log.getRequest(requestNo);
		if (request == null)
			return null;
		return byteArrayToObject(request);
	}

	@Override
	public final SortedMap<Integer, Object> getRequests() {
		SortedMap<Integer, byte[]> ba = log.getRequests();
		SortedMap<Integer, Object> o = new TreeMap<Integer, Object>();
		for (Entry<Integer, byte[]> e : ba.entrySet())
			o.put(e.getKey(), byteArrayToObject(e.getValue()));
		return o;
	}

	@Override
	public final SortedMap<Integer, Object> getRequests(int startingNo, int finishingNo) {
		SortedMap<Integer, byte[]> ba = log.getRequests(startingNo, finishingNo);
		SortedMap<Integer, Object> o = new TreeMap<Integer, Object>();
		for (Entry<Integer, byte[]> e : ba.entrySet())
			o.put(e.getKey(), byteArrayToObject(e.getValue()));
		return o;
	}
}

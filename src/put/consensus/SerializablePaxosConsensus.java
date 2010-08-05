package put.consensus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import put.consensus.listeners.CommitListener;
import put.consensus.listeners.ConsensusListener;
import put.consensus.listeners.RecoveryListener;

import lsr.common.Configuration;
import lsr.common.Request;
import lsr.paxos.client.Client;
import lsr.paxos.replica.Replica;
import lsr.paxos.replica.Replica.CrashModel;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.DiscWriter;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.SynchronousStableStorage;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.service.AbstractService;

public class SerializablePaxosConsensus extends AbstractService implements
		Consensus, Commitable {

	private Replica replica;
	private Client client;

	private BlockingQueue<byte[]> objectsToPropose = new LinkedBlockingQueue<byte[]>();
	private BlockingQueue<Runnable> operationsToBeDone = new LinkedBlockingQueue<Runnable>();

	private List<ConsensusListener> consensusListeners = new Vector<ConsensusListener>();
	private List<RecoveryListener> recoveryListeners = new Vector<RecoveryListener>();
	private List<CommitListener> commitListeners = new Vector<CommitListener>();

	private int lastDeliveredDecision = -1;

	// These classes should not be here - these are internal PaxosJava classes.
	private DiscWriter discWriter;
	private Log log;

	public SerializablePaxosConsensus(Configuration configuration, int localId)
			throws IOException {
		replica = new Replica(configuration, localId, this);
		replica.setCrashModel(CrashModel.FullStableStorage);
		replica.setLogPath("consensusLogs/" + localId);
	}

	@Override
	public void start() throws IOException {
		// These classes should not be here - these are internal PaxosJava
		// classes.
		StableStorage ss = replica.start().getStableStorage();
		log = ss.getLog();
		discWriter = ((SynchronousStableStorage) ss)._writer;

		client = new Client();
		client.connect();

		startThreads();
	}

	private void startThreads() {
		// Starting thread for new proposals
		new Thread() {
			public void run() {
				try {
					while (true)
						client.execute(objectsToPropose.take());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}.start();

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
	public final byte[] execute(final byte[] value, final int instanceId) {
		operationsToBeDone.add(new Runnable() {
			@Override
			public void run() {
				Object val = byteArrayToObject(value);
				for (ConsensusListener l : consensusListeners)
					l.decide(val);
				lastDeliveredDecision = instanceId;
			}
		});
		return new byte[0];
	}

	@Override
	public void propose(Object obj) {
		objectsToPropose.add(byteArrayFromObject(obj));
	}

	@Override
	public void commit(final Object commitData) {
		operationsToBeDone.add(new Runnable() {
			@Override
			public void run() {
				for (CommitListener listner : commitListeners)
					listner.onCommit(commitData);
				replica.onSnapshotMade(lastDeliveredDecision,
						byteArrayFromObject(commitData));
			}
		});
	}

	@Override
	public void updateToSnapshot(final int instanceId, final byte[] snapshot) {
		operationsToBeDone.add(new Runnable() {
			@Override
			public void run() {
				lastDeliveredDecision = instanceId;
				for (RecoveryListener listner : recoveryListeners)
					listner.recoverFromCommit(byteArrayToObject(snapshot));
			}
		});
	}

	@Override
	public void recoveryFinished() {
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
	public void addConsensusListener(ConsensusListener listener) {
		consensusListeners.add(listener);
	}

	@Override
	public void removeConsensusListener(ConsensusListener listener) {
		consensusListeners.remove(listener);
	}

	@Override
	public boolean addCommitListener(CommitListener listener) {
		return commitListeners.add(listener);
	}

	@Override
	public boolean removeCommitListener(CommitListener listener) {
		return commitListeners.remove(listener);
	}

	@Override
	public boolean addRecoveryListener(RecoveryListener listener) {
		return recoveryListeners.add(listener);
	}

	@Override
	public boolean removeRecoveryListener(RecoveryListener listener) {
		return recoveryListeners.remove(listener);
	}

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

	@Override
	public int highestInstance() {
		return log.getNextId() - 1;
	}

	@Override
	public ConsensusStateAndValue instanceValue(Integer instanceId)
			throws StorageException {
		// Next 'if' is critical
		if (instanceId >= log.getNextId())
			return null;

		// Getting instance and it's state
		ConsensusInstance ci = log.getInstance(instanceId);
		ConsensusStateAndValue consensusStateAndValue = new ConsensusStateAndValue();
		consensusStateAndValue.state = ci.getState();

		if (ci.getState() == LogEntryState.UNKNOWN)
			return consensusStateAndValue;

		// If there is a value, we should extract it
		try {

			// The value is wrapped by batching
			byte[] value = ci.getValue();
			byte[] request = Arrays.copyOfRange(value, 8, value.length);

			// The value is also wrapped by proposing
			Request r = Request.create(request);
			ObjectInputStream ois = new ObjectInputStream(
					new ByteArrayInputStream(r.getValue()));
			consensusStateAndValue.value = ois.readObject();

		} catch (IOException e) {
			throw new StorageException(
					"You managed to get an exception while reading RAM. Congratulations!",
					e);
		} catch (ClassNotFoundException e) {
			throw new StorageException(e);
		}

		return consensusStateAndValue;
	}

	@Override
	public void log(Object key, Object value) throws StorageException {
		discWriter.record(key, value);

	}

	@Override
	public Object retrieve(Object key) throws StorageException {
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
	public void askForSnapshot(int lastSnapshotInstance) {
	}

	@Override
	public void forceSnapshot(int lastSnapshotInstance) {
	}

	@Override
	public void instanceExecuted(int instanceId) {
	}
}

package put.consensus;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import lsr.common.Configuration;
import lsr.paxos.client.Client;
import lsr.paxos.replica.Replica;
import lsr.paxos.replica.Replica.CrashModel;
import lsr.paxos.storage.PublicDiscWriter;
import lsr.service.SerializableService;
import put.consensus.listeners.ConsensusListener;

@Deprecated
public class PaxosConsensus extends SerializableService implements Consensus {

    // The replica part
    private Replica replica;

    // The client part
    // as for now, one client == one consecutive sequence of proposals
    private Client client;

    // Here we collect objects waiting for proposing
    private BlockingQueue<Object> objectsToPropose = new LinkedBlockingQueue<Object>();

    // Log & writer - for storage.
    private PublicDiscWriter discWriter;

    // Thread for asynchronous (to the main application) proposing
    class ProposingThread extends Thread {
        public void run() {
            try {
                while (true)
                    client.execute(byteArrayFromObject(objectsToPropose.take()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Constructor. Basic usage:
     * <p>
     * <code> PaxosConsensus consensus = new PaxosConsensus(new Configuration(), localId, new ConsensusListenerImpl()); </code>
     * </p>
     * 
     * @param configuration of the nodes. See {@link Configuration}
     * @param localId the ID of the replica
     * @param listener default decision listener. Might be null, but this would
     *            mean that some of the decisions from recovery phase will get
     *            lost.
     * @throws IOException
     */
    public PaxosConsensus(Configuration configuration, int localId, ConsensusListener listener)
            throws IOException {
        if (listener != null)
            addConsensusListener(listener);
        // Starting replica
        replica = new Replica(configuration, localId, this);

        // Setting replica up
        replica.setCrashModel(CrashModel.FullStableStorage);
        // Log directory (!!!)
        replica.setLogPath("consensusLogs");
    }

    public void start() throws IOException {
        // Starting replica (and extracting in an inhuman way protected
        // PaxosJava classes)
        replica.start();
        discWriter = replica.getPublicDiscWriter();

        // Starting client
        client = new Client();
        client.connect();

        // Starting thread for new proposals
        new ProposingThread().start();
    }

    public void propose(Object obj) {
        objectsToPropose.add(obj);
    }

    // STORAGE

    public void log(Serializable key, Serializable value) throws StorageException {
        discWriter.record(key, value);
    }

    public Object retrieve(Serializable key) throws StorageException {
        return discWriter.retrive(key);
    }

    // public ConsensusStateAndValue instanceValue(Integer instanceId)
    // throws StorageException {
    // // Next 'if' is critical
    // if (instanceId >= log.getNextId())
    // return null;
    //
    // // Getting instance and it's state
    // ConsensusInstance ci = log.getInstance(instanceId);
    // ConsensusStateAndValue consensusStateAndValue = new
    // ConsensusStateAndValue();
    // consensusStateAndValue.state = ci.getState();
    //
    // if (ci.getState() == LogEntryState.UNKNOWN)
    // return consensusStateAndValue;
    //
    // // If there is a value, we should extract it
    // try {
    //
    // // The value is wrapped by batching
    // byte[] value = ci.getValue();
    // byte[] request = Arrays.copyOfRange(value, 8, value.length);
    //
    // // The value is also wrapped by proposing
    // Request r = Request.create(request);
    // ObjectInputStream ois = new ObjectInputStream(
    // new ByteArrayInputStream(r.getValue()));
    // consensusStateAndValue.value = ois.readObject();
    //
    // } catch (IOException e) {
    // throw new StorageException(
    // "You managed to get an exception while reading RAM. Congratulations!",
    // e);
    // } catch (ClassNotFoundException e) {
    // throw new StorageException(e);
    // }
    //
    // return consensusStateAndValue;
    // }
    //
    // @Override
    // public int highestInstance() {
    // return log.getNextId() - 1;
    // }

    // LISTENERS

    private CopyOnWriteArrayList<ConsensusListener> listeners = new CopyOnWriteArrayList<ConsensusListener>();

    public void addConsensusListener(ConsensusListener listener) {
        listeners.add(listener);
    }

    public void removeConsensusListener(ConsensusListener listener) {
        listeners.remove(listener);
    }

    // SERVICE

    @Override
    protected Object execute(Object value) {
        for (ConsensusListener listener : listeners) {
            listener.decide(value);
        }
        return new byte[0];
    }

    @Override
    protected Object makeObjectSnapshot() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    protected void updateToSnapshot(Object snapshot) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public ConsensusDelegateProposer getNewDelegateProposer() {
        throw new RuntimeException("Not implemented!");
    }

    @Override
    public int getHighestExecuteSeqNo() {
        throw new RuntimeException("Not implemented!");
    }
}

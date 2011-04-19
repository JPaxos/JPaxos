package lsr.paxos;

import java.io.IOException;
import java.util.BitSet;
import java.util.Deque;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.Dispatcher.Priority;
import lsr.common.DispatcherImpl;
import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.paxos.Proposer.ProposerState;
import lsr.paxos.events.ProposeEvent;
import lsr.paxos.events.StartProposerEvent;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.GenericNetwork;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.network.TcpNetwork;
import lsr.paxos.network.UdpNetwork;
import lsr.paxos.statistics.ReplicaStats;
import lsr.paxos.statistics.ThreadTimes;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

/**
 * Implements state machine replication. It keeps a replicated log internally
 * and informs the listener of decisions using callbacks. This implementation is
 * monolithic, in the sense that leader election/view change are integrated on
 * the paxos protocol.
 * 
 * <p>
 * The first consensus instance is 0. Decisions might not be reached in sequence
 * number order.
 * </p>
 */
public class PaxosImpl implements Paxos {
    private final ProposerImpl proposer;
    private final Acceptor acceptor;
    private final Learner learner;
    private final DecideCallback decideCallback;

    /**
     * Threading model - This class uses an event-driven threading model. It
     * starts a Dispatcher thread that is responsible for executing the
     * replication protocol and has exclusive access to the internal data
     * structures. The Dispatcher receives work using the pendingEvents queue.
     */
    /**
     * The Dispatcher thread executes the replication protocol. It receives and
     * executes events placed on the pendingEvents queue: messages from other
     * processes or proposals from the local process.
     * 
     * Only this thread is allowed to access the state of the replication
     * protocol. Therefore, there is no need for synchronization when accessing
     * this state. The synchronization is handled by the
     * <code>pendingEvents</code> queue.
     */

    private final Dispatcher dispatcher;
    private final Storage storage;
    // udpNetwork is used by the failure detector, so it is always created
    private final UdpNetwork udpNetwork;
    // Can be a udp, tcp or generic network. Only a single udpnetwork is created,
    // so the object held by udpNetwork might be reusued here, directly or via GenericNetwork
    private final Network network;
    private final FailureDetector failureDetector;
    private final CatchUp catchUp;
    private final SnapshotMaintainer snapshotMaintainer;
    private final Batcher batcher;

    /**
     * Initializes new instance of {@link PaxosImpl}.
     * 
     * @param decideCallback - the class that should be notified about
     *            decisions.
     * @param snapshotProvider
     * @param storage - the state of the paxos protocol
     * 
     * @throws IOException if an I/O error occurs
     */
    public PaxosImpl(DecideCallback decideCallback, SnapshotProvider snapshotProvider,
                     Storage storage) throws IOException {
        this.decideCallback = decideCallback;
        this.storage = storage;
        ProcessDescriptor p = ProcessDescriptor.getInstance();

        // Used to collect statistics. If the benchmarkRun==false, these
        // method initialize an empty implementation of ReplicaStats and ThreadTimes, 
        // which effectively disables collection of statistics
        ReplicaStats.initialize(p.numReplicas, p.localId);
        ThreadTimes.initialize();

        // Handles the replication protocol and writes messages to the network
        dispatcher = new DispatcherImpl("Dispatcher");        

        if (snapshotProvider != null) {
            logger.info("Starting snapshot maintainer");
            snapshotMaintainer = new SnapshotMaintainer(this.storage, dispatcher, snapshotProvider);
            storage.getLog().addLogListener(snapshotMaintainer);
        } else {
            logger.info("No snapshot support");
            snapshotMaintainer = null;
        }

        // UDPNetwork is always needed because of the failure detector
        this.udpNetwork = new UdpNetwork();
        if (p.network.equals("TCP")) {
            network = new TcpNetwork();
        } else if (p.network.equals("UDP")) {
            network = udpNetwork;
        } else if (p.network.equals("Generic")) {
            TcpNetwork tcp = new TcpNetwork();
            network = new GenericNetwork(tcp, udpNetwork);
        } else {
            throw new IllegalArgumentException("Unknown network type: " + p.network +
                                               ". Check paxos.properties configuration.");
        }
        logger.info("Network: " + network.getClass().getCanonicalName());

        catchUp = new CatchUp(snapshotProvider, this, this.storage, network);
        failureDetector = new FailureDetector(this, udpNetwork, this.storage);

        // create acceptors and learners
        proposer = new ProposerImpl(this, network, failureDetector, this.storage, p.crashModel);
        acceptor = new Acceptor(this, this.storage, network);
        learner = new Learner(this, proposer, this.storage);

        // batching utility
        batcher = new BatcherImpl();
    }

    /**
     * Joins this process to the paxos protocol. The catch-up and failure
     * detector mechanisms are started and message handlers are registered.
     */
    public void startPaxos() {
        MessageHandler handler = new MessageHandlerImpl();
        Network.addMessageListener(MessageType.Alive, handler);
        Network.addMessageListener(MessageType.Propose, handler);
        Network.addMessageListener(MessageType.Prepare, handler);
        Network.addMessageListener(MessageType.PrepareOK, handler);
        Network.addMessageListener(MessageType.Accept, handler);
        
        // Starts the threads on the child modules. Should be done after 
        // all the dependencies are established, ie. listeners registered.
        udpNetwork.start();
        network.start();
        catchUp.start();
        failureDetector.start();
        dispatcher.start();
    }

    /**
     * Proposes new value to paxos protocol.
     * 
     * This process has to be a leader to call this method. If the process is
     * not a leader, exception is thrown.
     * 
     * @param value - the value to propose
     * @throws NotLeaderException if the process is not a leader
     */
    public void propose(Request value) throws NotLeaderException {
        if (!isLeader()) {
            throw new NotLeaderException("Cannot propose: local process is not the leader");
        }
        dispatcher.dispatch(new ProposeEvent(proposer, value));
    }

    /**
     * Adds {@link StartProposerEvent} to current dispatcher which starts the
     * proposer on current replica.
     */
    public void startProposer() {
        assert proposer.getState() == ProposerState.INACTIVE : "Already in proposer role.";

        StartProposerEvent event = new StartProposerEvent(proposer);
        if (dispatcher.amIInDispatcher()) {
            event.run();
        } else {
            dispatcher.dispatch(event);
        }
    }

    /**
     * Is this process on the role of leader?
     * 
     * @return <code>true</code> if current process is the leader;
     *         <code>false</code> otherwise
     */
    public boolean isLeader() {
        return getLeaderId() == ProcessDescriptor.getInstance().localId;
    }

    /**
     * Gets the id of the replica which is currently the leader.
     * 
     * @return id of replica which is leader
     */
    public int getLeaderId() {
        return storage.getView() % ProcessDescriptor.getInstance().numReplicas;
    }

    /**
     * Gets the dispatcher used by paxos to avoid concurrency in handling
     * events.
     * 
     * @return current dispatcher object
     */
    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    /**
     * Changes state of specified consensus instance to <code>DECIDED</code>.
     * 
     * @param instanceId - the id of instance that has been decided
     */
    public void decide(int instanceId) {
        assert dispatcher.amIInDispatcher() : "Incorrect thread: " + Thread.currentThread();

        ConsensusInstance ci = storage.getLog().getInstance(instanceId);
        assert ci != null : "Deciding on instance already removed from logs";
        assert ci.getState() != LogEntryState.DECIDED : "Deciding on already decided instance";

        ci.setDecided();

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Decided " + instanceId + ", Log Size: " + storage.getLog().size());
        }
        
        // Benchmark. If the configuration property benchmarkRun is false,
        // these are empty calls.
        ReplicaStats.getInstance().consensusEnd(instanceId);
        ThreadTimes.getInstance().startInstance(instanceId+1);
        
        storage.updateFirstUncommitted();

        if (isLeader()) {
            proposer.stopPropose(instanceId);
            proposer.ballotFinished();
        } else {
            // not leader. Should we start the catchup?
            if (ci.getId() > storage.getFirstUncommitted() +
                             ProcessDescriptor.getInstance().windowSize) {
                // The last uncommitted value was already decided, since
                // the decision just reached is outside the ordering window
                // So start catchup.
                catchUp.startCatchup();
            }
        }

        Deque<Request> requests = batcher.unpack(ci.getValue());
        decideCallback.onRequestOrdered(instanceId, requests);
    }

    /**
     * Increases the view of this process to specified value. The new view has
     * to be greater than the current one.
     * 
     * @param newView - the new view number
     */
    public void advanceView(int newView) {
        assert dispatcher.amIInDispatcher();
        assert newView > storage.getView() : "Can't advance to the same or lower view";

        logger.info("Advancing to view " + newView + ", Leader=" +
                    (newView % ProcessDescriptor.getInstance().numReplicas));

        ReplicaStats.getInstance().advanceView(newView);

        if (isLeader()) {
            proposer.stopProposer();
        }

        /*
         * TODO: NS [FullSS] don't sync to disk at this point.
         */
        storage.setView(newView);

        assert !isLeader() : "Cannot advance to a view where process is leader by receiving a message";
        failureDetector.leaderChange(getLeaderId());
    }

    // *****************
    // Auxiliary classes
    // *****************
    /**
     * Receives messages from other processes and stores them on the
     * pendingEvents queue for processing by the Dispatcher thread.
     */
    private class MessageHandlerImpl implements MessageHandler {
        public void onMessageReceived(Message msg, int sender) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("Msg rcv: " + msg);
            }
            MessageEvent event = new MessageEvent(msg, sender);

            // prioritize Alive messages
            if (msg instanceof Alive) {
                dispatcher.dispatch(event, Priority.High);
            } else {
                dispatcher.dispatch(event);
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
            // Empty
        }
    }

    private class MessageEvent implements Runnable {
        private final Message msg;
        private final int sender;

        public MessageEvent(Message msg, int sender) {
            this.msg = msg;
            this.sender = sender;
        }

        public void run() {
            try {
                // The monolithic implementation of Paxos does not need Nack
                // messages because the Alive messages from the failure detector
                // are handled by the Paxos algorithm, so it can advance view
                // when it receives Alive messages. But in the modular
                // implementation, the Paxos algorithm does not use Alive
                // messages,
                // so if a process p is on a lower view and the system is idle,
                // p will remain in the lower view until there is another
                // request
                // to be ordered. The Nacks are required to force the process to
                // advance

                // Ignore any message with a lower view.
                if (msg.getView() < storage.getView())
                    return;

                if (msg.getView() > storage.getView()) {
                    assert msg.getType() != MessageType.PrepareOK : "Received PrepareOK for view " +
                                                                    msg.getView() +
                                                                    " without having sent a Prepare";
                    advanceView(msg.getView());
                }

                // Invariant for all message handlers: msg.view >= view
                switch (msg.getType()) {
                    case Prepare:
                        acceptor.onPrepare((Prepare) msg, sender);
                        break;

                    case PrepareOK:
                        if (proposer.getState() == ProposerState.INACTIVE) {
                            logger.fine("Not in proposer role. Ignoring message");
                        } else {
                            proposer.onPrepareOK((PrepareOK) msg, sender);
                        }
                        break;

                    case Propose:
                        acceptor.onPropose((Propose) msg, sender);
                        if (!storage.isInWindow(((Propose) msg).getInstanceId()))
                            activateCatchup();
                        break;

                    case Accept:
                        learner.onAccept((Accept) msg, sender);
                        break;

                    case Alive:
                        // The function checkIfCatchUpNeeded also creates
                        // missing logs
                        if (!isLeader() && checkIfCatchUpNeeded(((Alive) msg).getLogSize()))
                            activateCatchup();
                        break;

                    default:
                        logger.warning("Unknown message type: " + msg);
                }
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "Unexpected exception", t);
                System.exit(1);
            }
        }

        /**
         * After getting an alive message, we need to check whether we're up to
         * date.
         * 
         * @param logSize - the actual size of the log
         */
        private boolean checkIfCatchUpNeeded(int logSize) {
            Log log = storage.getLog();

            if (log.getNextId() < logSize) {

                // If we got information, that a newer instance exists, we can
                // create it
                log.getInstance(logSize - 1);
            }

            // We check if all ballots outside the window finished
            int i = storage.getFirstUncommitted();
            for (; i < log.getNextId() - ProcessDescriptor.getInstance().windowSize; i++) {
                if (log.getInstance(i).getState() != LogEntryState.DECIDED)
                    return true;
            }
            return false;

        }

        private void activateCatchup() {
            synchronized (catchUp) {
                catchUp.notify();
            }
        }
    }

    public void onSnapshotMade(Snapshot snapshot) {
        snapshotMaintainer.onSnapshotMade(snapshot);
    }

    /**
     * Returns the storage with the current state of paxos protocol.
     * 
     * @return the storage
     */
    public Storage getStorage() {
        return storage;
    }

    public Network getNetwork() {
        return network;
    }

    /**
     * Returns the catch-up mechanism used by paxos protocol.
     * 
     * @return the catch-up mechanism
     */
    public CatchUp getCatchup() {
        return catchUp;
    }

    public Proposer getProposer() {
        return proposer;
    }

    private final static Logger logger = Logger.getLogger(PaxosImpl.class.getCanonicalName());
}

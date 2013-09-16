package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.util.BitSet;

import lsr.common.RequestType;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.ActiveFailureDetector;
import lsr.paxos.Batcher;
import lsr.paxos.FailureDetector;
import lsr.paxos.NewPassiveBatcher;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotMaintainer;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.core.Proposer.ProposerState;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.GenericNetwork;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.MulticastNetwork;
import lsr.paxos.network.Network;
import lsr.paxos.network.NioNetwork;
import lsr.paxos.network.TcpNetwork;
import lsr.paxos.network.UdpNetwork;
import lsr.paxos.replica.ClientRequestManager;
import lsr.paxos.replica.DecideCallback;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Paxos implements FailureDetector.FailureDetectorListener {
    private final ProposerImpl proposer;
    private final Acceptor acceptor;
    private final Learner learner;
    private DecideCallback decideCallback;

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

    private final SingleThreadDispatcher dispatcher;
    private final Storage storage;

    // Can be a udp, tcp or generic network.
    private final Network network;

    private final FailureDetector failureDetector;
    private final CatchUp catchUp;
    private final SnapshotMaintainer snapshotMaintainer;

    /** Receives, queues and creates batches with client requests. */
    private final Batcher batcher;
    protected boolean active = false;

    /**
     * Initializes new instance of {@link Paxos}.
     * 
     * @param decideCallback - the class that should be notified about
     *            decisions.
     * @param snapshotProvider
     * @param storage - the state of the paxos protocol
     * 
     * @throws IOException if an I/O error occurs
     */
    public Paxos(SnapshotProvider snapshotProvider, Storage storage) throws IOException {
        this.storage = storage;

        this.dispatcher = new SingleThreadDispatcher("Protocol");

        if (snapshotProvider != null) {
            logger.info("Starting snapshot maintainer");
            snapshotMaintainer = new SnapshotMaintainer(this.storage, dispatcher, snapshotProvider);
            storage.getLog().addLogListener(snapshotMaintainer);
        } else {
            logger.error("!!! No snapshot support !!!");
            snapshotMaintainer = null;
        }

        UdpNetwork udpNetwork = null;

        if (processDescriptor.network.equals("TCP")) {
            network = new TcpNetwork();
            // for FD
            udpNetwork = new UdpNetwork();
        } else if (processDescriptor.network.equals("NIO")) {
            network = new NioNetwork();
            // for FD
            udpNetwork = new UdpNetwork();
        } else if (processDescriptor.network.equals("UDP")) {
            network = new UdpNetwork();
        } else if (processDescriptor.network.equals("Multicast")) {
            // for unicast messages, still using TCP
            TcpNetwork tcpNetwork = new TcpNetwork();
            network = new MulticastNetwork(tcpNetwork, storage.getRunUniqueId());
        } else if (processDescriptor.network.equals("Generic")) {
            TcpNetwork tcpNetwork = new TcpNetwork();
            udpNetwork = new UdpNetwork();
            network = new GenericNetwork(tcpNetwork, udpNetwork);
        } else {
            throw new IllegalArgumentException("Unknown network type: " +
                                               processDescriptor.network +
                                               ". Check paxos.properties configuration.");
        }
        logger.info("Network: {}", network.getClass().getCanonicalName());

        catchUp = new CatchUp(snapshotProvider, this, this.storage, network);

        // If the network is not suitable for FD, udpNetwork is created
        failureDetector = new ActiveFailureDetector(this,
                udpNetwork == null ? network : udpNetwork, this.storage);

        // create proposer, acceptor and learner
        proposer = new ProposerImpl(this, network, this.storage, processDescriptor.crashModel);
        acceptor = new Acceptor(this, this.storage, network);
        learner = new Learner(this, this.storage);

        batcher = new NewPassiveBatcher(this);

        if (udpNetwork != null)
            udpNetwork.start();
        network.start();
        dispatcher.start();
    }

    public void setDecideCallback(DecideCallback decideCallback) {
        this.decideCallback = decideCallback;
        batcher.setDecideCallback(decideCallback);
    }

    public void setClientRequestManager(ClientRequestManager requestManager) {
        proposer.setClientRequestManager(requestManager);
    }

    /**
     * Joins this process to the paxos protocol. The catch-up and failure
     * detector mechanisms are started and message handlers are registered.
     */
    public void startActivePaxos() {
        assert decideCallback != null : "Cannot start with null DecideCallback";

        logger.info("start active Paxos");

        // Starts the threads on the child modules. Should be done after
        // all the dependencies are established, ie. listeners registered.
        batcher.start();
        proposer.start();
        failureDetector.start(storage.getView());

        active = true;

        suspect(0);
    }

    /**
     * Joins this process to the paxos protocol. The catch-up and failure
     * detector mechanisms are started and message handlers are registered.
     */
    public void startPassivePaxos() {
        assert decideCallback != null : "Cannot start with null DecideCallback";

        logger.info("starting passive Paxos");
        MessageHandler handler = new MessageHandlerImpl();
        Network.addMessageListener(MessageType.Alive, handler);
        Network.addMessageListener(MessageType.Propose, handler);
        Network.addMessageListener(MessageType.Prepare, handler);
        Network.addMessageListener(MessageType.PrepareOK, handler);
        Network.addMessageListener(MessageType.Accept, handler);
    }

    /**
     * Proposes new value to paxos protocol.
     * 
     * This process has to be a leader to call this method. If the process is
     * not a leader, exception is thrown.
     * 
     * @param request - the value to propose
     */
    public void enqueueRequest(RequestType request) {
        // called by one of the Selector threads.
        batcher.enqueueClientRequest(request);
    }

    public Batcher getBatcher() {
        return batcher;
    }

    public byte[] requestBatch() {
        return batcher.requestBatch();
    }

    public void startProposer() {
        assert dispatcher.amIInDispatcher() : "Incorrect thread: " + Thread.currentThread();
        assert proposer.getState() == ProposerState.INACTIVE : "Already in proposer role.";

        proposer.prepareNextView();
    }

    /**
     * Is this process on the role of leader?
     * 
     * @return <code>true</code> if current process is the leader;
     *         <code>false</code> otherwise
     */
    public boolean isLeader() {
        return processDescriptor.isLocalProcessLeader(storage.getView());
    }

    /**
     * Gets the id of the replica which is currently the leader.
     * 
     * @return id of replica which is leader
     */
    public int getLeaderId() {
        return processDescriptor.getLeaderOfView(storage.getView());
    }

    /**
     * Gets the dispatcher used by paxos to avoid concurrency in handling
     * events.
     * 
     * @return current dispatcher object
     */
    public SingleThreadDispatcher getDispatcher() {
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

        logger.info(processDescriptor.logMark_OldBenchmark, "Decided {}", instanceId);

        storage.updateFirstUncommitted();

        if (isLeader()) {
            proposer.stopPropose(instanceId);
            proposer.ballotFinished();
        } else {
            // not leader. Should we start the catch-up?
            if (ci.getId() > storage.getFirstUncommitted() + processDescriptor.windowSize) {
                // The last uncommitted value was already decided, since
                // the decision just reached is outside the ordering window
                // So start catch-up.
                catchUp.forceCatchup();
            }
        }

        decideCallback.onRequestOrdered(instanceId, ci);
    }

    /**
     * Increases the view of this process to specified value. The new view has
     * to be greater than the current one.
     * 
     * This method is executed when this replica receives a message from a
     * higher view, so the replica is not the leader of newView.
     * 
     * This may be called before the view is prepared.
     * 
     * @param newView - the new view number
     */
    public void advanceView(int newView) {
        assert dispatcher.amIInDispatcher();
        int oldView = storage.getView();
        assert newView > oldView : "Can't advance to the same or lower view";

        if (logger.isInfoEnabled()) {
            logger.info("Advancing to view {} from {}, Leader={}", newView, oldView,
                    (newView % processDescriptor.numReplicas));
        }

        if (isLeader()) {
            batcher.suspendBatcher();
            proposer.stopProposer();
        }

        storage.setView(newView);
        // line above changed the leader

        assert !isLeader() : "Cannot advance to a view where process is leader by receiving a message.";
    }

    @Override
    public void suspect(final int view) {
        logger.warn(processDescriptor.logMark_Benchmark, "Suspecting {} on view {}",
                processDescriptor.getLeaderOfView(view), view);
        // Called by the Failure detector thread. Dispatch to the protocol
        // thread
        dispatcher.submit(new Runnable() {
            @Override
            public void run() {
                // The view may have changed since this task was scheduled.
                // If so, ignore this suspicion.
                if (view == storage.getView()) {
                    startProposer();
                } else {
                    logger.info("Ignoring suspicion for view {}. Current view: {}", view,
                            storage.getView());
                }
            }
        });
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
            logger.debug("Msg rcv: {}", msg);

            MessageEvent event = new MessageEvent(msg, sender);
            dispatcher.submit(event);
        }

        public void onMessageSent(Message message, BitSet destinations) {
            // Empty
        }
    }

    private final class MessageEvent implements Runnable {
        private final Message msg;
        private final int sender;

        public MessageEvent(Message msg, int sender) {
            this.msg = msg;
            this.sender = sender;
        }

        public void run() {
            try {
                /*
                 * Ignore any message with a lower view. Pass alive, it contains
                 * log size; may be useful and is harmless
                 */
                if (msg.getView() < storage.getView() && !(msg instanceof Alive)) {
                    logger.debug("Ignoring message. Current view: {}, Message: ",
                            storage.getView(), msg);
                    return;
                }

                if (msg.getView() > storage.getView()) {
                    if (msg.getType() == MessageType.PrepareOK) {
                        logger.error("Theoretically it can happen. If you ever see this message, tell JK");
                        return;
                    }
                    advanceView(msg.getView());
                }

                switch (msg.getType()) {
                    case Prepare:
                        acceptor.onPrepare((Prepare) msg, sender);
                        break;

                    case PrepareOK:
                        if (proposer.getState() == ProposerState.INACTIVE) {
                            logger.debug("Not in proposer role. Ignoring message {}", msg);
                        } else {
                            proposer.onPrepareOK((PrepareOK) msg, sender);
                        }
                        break;

                    case Propose:
                        acceptor.onPropose((Propose) msg, sender);
                        if (!storage.isInWindow(((Propose) msg).getInstanceId())) {
                            activateCatchup();
                        }
                        break;

                    case Accept:
                        learner.onAccept((Accept) msg, sender);
                        break;

                    case Alive:
                        if (!isLeader() && checkIfCatchUpNeeded(((Alive) msg).getLogNextId())) {
                            activateCatchup();
                        }
                        break;

                    default:
                        logger.error("Unknown message type: {}", msg);
                        assert false : msg;
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }

        /**
         * After getting an alive message, we need to check whether we're up to
         * date.
         * 
         * @param aliveNextId - the actual size of the log
         */
        private boolean checkIfCatchUpNeeded(int aliveNextId) {
            Log log = storage.getLog();

            if (log.getNextId() < aliveNextId) {

                // If we got information, that a newer instance exists, we can
                // create it
                log.getInstance(aliveNextId - 1);
            }

            // We check if all ballots outside the window finished
            int i = storage.getFirstUncommitted();
            for (; i < log.getNextId() - processDescriptor.windowSize; i++) {
                if (log.getInstance(i).getState() != LogEntryState.DECIDED) {
                    return true;
                }
            }
            return false;

        }

        private void activateCatchup() {
            catchUp.forceCatchup();
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

    public void onViewPrepared(int nextInstanceId) {
        batcher.resumeBatcher(nextInstanceId);
    }

    public boolean isActive() {
        return active;
    }

    private final static Logger logger = LoggerFactory.getLogger(Paxos.class);
}

package lsr.paxos.storage;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.paxos.UnBatcher;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientBatchManager.FwdBatchRetransmitter;

/**
 * Contains data related with one consensus instance.
 */
public class InMemoryConsensusInstance implements Serializable, ConsensusInstance {
    private static final long serialVersionUID = 1L;
    protected final int id;

    protected int lastSeenView;
    protected int lastVotedView;

    protected byte[] value;

    protected LogEntryState state;

    protected transient BitSet accepts = new BitSet();

    // For indirect consensus:
    protected transient boolean decidable = false;
    protected transient FwdBatchRetransmitter fbr = null;

    /**
     * Initializes new instance of consensus with all values specified.
     * 
     * @param id - the id of instance to create
     * @param state - the state of consensus
     * @param view - the view of last message in this consensus
     * @param value - the value accepted or decided in this instance
     */
    /* package private */ InMemoryConsensusInstance(int id, LogEntryState state, int acceptedView,
                                                    byte[] acceptedValue) {
        this.id = id;
        this.state = state;

        this.lastVotedView = acceptedView;
        this.lastSeenView = acceptedView;

        this.value = acceptedValue;

        onValueChange();

        assertInvariant();
    }

    /**
     * Initializes new empty instance of consensus. The initial state is set to
     * <code>UNKNOWN</code>, view to <code>-1</code> and value to
     * <code>null</code>.
     * 
     * @param id the id of instance to create
     */
    public InMemoryConsensusInstance(int id) {
        this(id, LogEntryState.UNKNOWN, -1, null);
    }

    /**
     * Initializes new instance of consensus from input stream. The input stream
     * should contain serialized instance created by <code>toByteArray()</code>
     * or <code>write(ByteBuffer)</code> method.
     * 
     * @param input - the input stream containing serialized consensus instance
     * @throws IOException the stream has been closed and the contained input
     *             stream does not support reading after close, or another I/O
     *             error occurs
     * @see #toByteArray()
     * @see #write(ByteBuffer)
     */
    public InMemoryConsensusInstance(DataInputStream input) throws IOException {
        this.id = input.readInt();
        this.lastVotedView = input.readInt();
        this.state = LogEntryState.values()[input.readInt()];

        if (this.state == LogEntryState.DECIDED)
            accepts = null;

        int size = input.readInt();
        if (size == -1) {
            value = null;
        } else {
            value = new byte[size];
            input.readFully(value);
        }

        lastSeenView = lastVotedView;

        onValueChange();

        assertInvariant();
    }

    public InMemoryConsensusInstance(ByteBuffer input) {
        this.id = input.getInt();
        this.lastVotedView = input.getInt();
        this.state = LogEntryState.values()[input.getInt()];

        if (this.state == LogEntryState.DECIDED)
            accepts = null;

        int size = input.getInt();
        if (size == -1) {
            value = null;
        } else {
            value = new byte[size];
            input.get(value);
        }

        lastSeenView = lastVotedView;

        onValueChange();

        assertInvariant();
    }

    private void assertInvariant() {
        assert lastSeenView >= lastVotedView;

        assert value == null ^ state != LogEntryState.UNKNOWN : "Invalid state. Value=" + value +
                                                                ": " + toString();

        assert state != LogEntryState.RESET || (lastSeenView != lastVotedView);
        assert state != LogEntryState.KNOWN || (lastSeenView == lastVotedView);
        assert state != LogEntryState.DECIDED || (lastSeenView == lastVotedView);

        assert accepts == null ^ state != LogEntryState.DECIDED;

        // TODO: more

    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.storage.ConsensusInstance#getId()
     */
    @Override
    public int getId() {
        return id;
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.storage.ConsensusInstance#updateStateFromAck(int, int)
     */
    @Override
    public boolean updateStateFromAccept(int view, int replicaId) {
        assert lastSeenView <= view : "Cannot set smaller view.";
        // assert state != LogEntryState.DECIDED || ( view == this.lastVotedView
        // && view == this.lastSeenView);

        switch (state) {
            case UNKNOWN:
                // an Accept message arrived before any Propose message
                lastSeenView = view;
                break;
            case KNOWN:
                if (lastSeenView < view) {
                    // an Accept arrived before a Propose in a higher view
                    lastSeenView = view;
                    accepts.clear();
                    state = LogEntryState.RESET;
                }
                break;
            case RESET:
                if (lastSeenView < view) {
                    // we missed a propose messages again and got an accept
                    lastSeenView = view;
                    accepts.clear();
                } else {
                    // another accept delivered before propose
                }
                break;
            case DECIDED:
                assert false;
                break;
        }
        accepts.set(replicaId);
        assertInvariant();

        return isReadyToBeDecieded();
    }

    @Override
    public int getLastSeenView() {
        return lastSeenView;
    }

    @Override
    public int getLastVotedView() {
        return lastVotedView;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public LogEntryState getState() {
        return state;
    }

    @Override
    public boolean isMajority() {
        return accepts == null || accepts.cardinality() >= processDescriptor.majority;
    }

    @Override
    public void setDecided() {
        state = LogEntryState.DECIDED;
        accepts = null;
        assertInvariant();
    }

    @Override
    public void writeAsLastVoted(ByteBuffer byteBuffer) {
        byteBuffer.putInt(id);
        byteBuffer.putInt(lastVotedView);
        byteBuffer.putInt(
                state != LogEntryState.RESET ? state.ordinal() : LogEntryState.KNOWN.ordinal());
        if (state == LogEntryState.UNKNOWN) {
            byteBuffer.putInt(-1);
        } else {
            byteBuffer.putInt(value.length);
            byteBuffer.put(value);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.storage.ConsensusInstance#byteSize()
     */
    @Override
    public int byteSize() {
        int size = (value == null ? 0 : value.length) + 4 /* length of array */;
        size += 3 * 4 /* ID, view and state */;
        return size;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        result = prime * result + lastVotedView;
        result = prime * result + lastSeenView;
        result = prime * result + Arrays.hashCode(value);
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InMemoryConsensusInstance other = (InMemoryConsensusInstance) obj;
        if (id != other.id)
            return false;
        if (state == null) {
            if (other.state != null)
                return false;
        } else if (!state.equals(other.state))
            return false;
        if (lastSeenView != other.lastSeenView)
            return false;
        if (lastVotedView != other.lastVotedView)
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.storage.ConsensusInstance#toString()
     */
    @Override
    public String toString() {
        return "(" + id + ", " + state +
               ", lastSeenView=" + lastSeenView +
               ", lastVotedView=" + lastVotedView +
               ", value=" + value + ")";
    }

    @Override
    public boolean updateStateFromPropose(int sender, int newView, byte[] newValue) {
        // Ignore any state update from an older view.
        if (newView < lastSeenView) {
            assert state == LogEntryState.DECIDED;
            return false;
        }
        switch (state) {
            case DECIDED:
                /*
                 * This can happen when the new leader re-proposes an instance
                 * that was decided by some processes on a previous view.
                 * 
                 * The value must be the same as the local value.
                 */
                assert Arrays.equals(newValue,
                        value) : "Values don't match. New view: " + newView +
                                 ", local: " + this + ", newValue: " +
                                 Arrays.toString(newValue) + ", old: " +
                                 Arrays.toString(value);
                break;

            case KNOWN:
                if (newView > lastSeenView) {
                    // the message is for a higher view
                    lastSeenView = newView;
                    lastVotedView = newView;
                    value = newValue;

                    aboutToVote();

                    accepts.clear();
                    accepts.set(sender);
                    accepts.set(processDescriptor.localId);

                    onValueChange();
                } else {
                    // we got retransmission of the propose
                    assert Arrays.equals(newValue, value) : "Values don't match. newView: " +
                                                            newView + ", local: " + this;

                    assert accepts.get(sender);
                }
                break;

            case RESET:
                // a reset instance means that we got an accept before the
                // propose
                lastSeenView = newView;
                lastVotedView = newView;
                value = newValue;

                state = LogEntryState.KNOWN;

                aboutToVote();

                // but we might have got the accept for an older view
                if (newView > lastSeenView) {
                    accepts.clear();
                }
                accepts.set(sender);
                accepts.set(processDescriptor.localId);

                onValueChange();

                break;
            case UNKNOWN:
                lastSeenView = newView;
                lastVotedView = newView;
                value = newValue;

                state = LogEntryState.KNOWN;

                aboutToVote();

                accepts.set(sender);
                accepts.set(processDescriptor.localId);

                onValueChange();
                break;

            default:
                throw new RuntimeException("Unknown instance state");
        }

        assertInvariant();

        return isReadyToBeDecieded();
    }

    @Override
    public void updateStateFromDecision(int newView, byte[] newValue) {
        assert newValue != null;
        if (state == LogEntryState.DECIDED) {
            logger.error("Updating a decided instance from a catchup message: {}", this);
            // The value must be the same as the local value. No change.
            assert Arrays.equals(newValue, value) : "Values don't match. New view: " +
                                                    newView +
                                                    ", local: " + this;
            return;
        } else {
            lastSeenView = newView;
            lastVotedView = newView;
            value = newValue;

            // shall not be decided, as Paxos.decide(instancId) will be called
            // afterwards
            this.state = LogEntryState.KNOWN;

            aboutToVote();

            onValueChange();
        }
        assertInvariant();
    }

    /**
     * Called when value and last.*View are set and the process is about to send
     * and Accept or Propose
     * 
     * Re-implemented by Synchronous CI
     */
    protected void aboutToVote() {
        assert value != null : "value cannot be null. View: " + lastSeenView;
        assert lastSeenView == lastVotedView;
        assert state == LogEntryState.KNOWN;
    }

    protected boolean isReadyToBeDecieded() {
        if (processDescriptor.indirectConsensus &&
            !ClientBatchStore.instance.hasAllBatches(getClientBatchIds()))
            return false;
        return isMajority() && state == LogEntryState.KNOWN;
    }

    /*
     * INDIRECT STUFF FOLLOWS
     */

    protected final void onValueChange() {
        if (value == null)
            return;
        if (processDescriptor.indirectConsensus) {
            for (ClientBatchID cbid : getClientBatchIds()) {
                ClientBatchStore.instance.associateWithInstance(cbid);
            }
        }
    }

    @Override
    public void setDecidable(boolean decidable) {
        this.decidable = decidable;
    }

    @Override
    public boolean isDecidable() {
        return LogEntryState.DECIDED.equals(state) || decidable;
    }

    protected transient Deque<ClientBatchID> cbids = null;

    @Override
    public Deque<ClientBatchID> getClientBatchIds() {
        assert processDescriptor.indirectConsensus;
        if (cbids == null)
            cbids = UnBatcher.unpackCBID(value);
        return cbids;
    }

    @Override
    public void setClientBatchIds(Deque<ClientBatchID> cbids) {
        this.cbids = cbids;
    }

    @Override
    public void stopFwdBatchForwarder() {
        if (fbr != null)
            ClientBatchStore.instance.getClientBatchManager().removeTask(fbr);
    }

    @Override
    public void setFwdBatchForwarder(FwdBatchRetransmitter fbr) {
        this.fbr = fbr;
    }

    private final static Logger logger = LoggerFactory.getLogger(InMemoryConsensusInstance.class);
}
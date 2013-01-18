package lsr.paxos.storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.paxos.Batcher;
import lsr.paxos.replica.ClientBatchID;

/**
 * Contains data related with one consensus instance.
 */
public class ConsensusInstance implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final int id;
    protected int view;
    protected byte[] value;
    protected LogEntryState state;

    protected transient BitSet accepts = new BitSet();
    protected transient boolean decidable = false;

    /**
     * Represents possible states of consensus instance.
     */
    public enum LogEntryState {
        /**
         * Represents the empty consensus state. There is no information about
         * current view nor value.
         */
        UNKNOWN,
        /**
         * The consensus in this state received the <code>PROPOSE</code> message
         * from the leader but hasn't received the majority of the
         * <code>ACCEPT</code> messages. In this state there is some view and
         * value specified, but they can be changed later.
         */
        KNOWN,
        /**
         * Represents state when {@link lsr.paxos.Learner} received majority of
         * <code>ACCEPT</code> message. In this state the view and value of
         * consensus instance cannot be changed.
         */
        DECIDED
    }

    /**
     * Initializes new instance of consensus with all value specified.
     * 
     * @param id - the id of instance to create
     * @param state - the state of consensus
     * @param view - the view of last message in this consensus
     * @param value - the value accepted or decided in this instance
     */
    public ConsensusInstance(int id, LogEntryState state, int view, byte[] value) {
        this.id = id;
        this.state = state;
        this.view = view;
        this.value = value;

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
    public ConsensusInstance(int id) {
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
    public ConsensusInstance(DataInputStream input) throws IOException {
        this.id = input.readInt();
        this.view = input.readInt();
        this.state = LogEntryState.values()[input.readInt()];

        int size = input.readInt();
        if (size == -1) {
            value = null;
        } else {
            value = new byte[size];
            input.readFully(value);
        }

        onValueChange();

        assertInvariant();
    }

    private void assertInvariant() {
        // If value is non null, the state must be either Decided or Known.
        // If value is null, it must be unknown
        assert value == null ^ state != LogEntryState.UNKNOWN : "Invalid state. Value=" + value +
                                                                ": " + toString();
    }

    /**
     * Gets the number of the consensus instance. Different instances should
     * have different id's.
     * 
     * @return id of instance
     */
    public int getId() {
        return id;
    }

    /**
     * Changes the view to the newest one. It cannot be changed to value less
     * than current view, and shouldn't be changed if the consensus is already
     * in <code>Decided</code> state.
     * 
     * Clears the accepts if the new view is higher than the current one.
     * 
     * @param view - the new view value
     */
    public void setView(int view) {
        assert this.view <= view : "Cannot set smaller view.";
        assert state != LogEntryState.DECIDED || view == this.view;
        if (this.view < view) {
            accepts.clear();
            this.view = view;
        }
    }

    /**
     * Gets the current view of this instance. The view of instance is
     * represented by the view of last message. If the current state of
     * consensus is decided, then view should not be changed.
     * 
     * @return the view number of this instance
     */
    public int getView() {
        return view;
    }

    /**
     * Sets new value holding by this instance. Each value has view in which it
     * is valid, so it has to be set here also.
     * 
     * @param view - the view number in which value is valid
     * @param value - the value which was accepted by this instance
     */
    protected void setValue(int view, byte[] value) {
        assert value != null : "value cannot be null. View: " + view;

        assert state != LogEntryState.DECIDED
               || Arrays.equals(this.value, value) : view + " " + value + " " + this;
        assert state != LogEntryState.KNOWN
               || view != this.view
               || Arrays.equals(this.value, value) : view + " " + value + " " + this;

        if (state == LogEntryState.UNKNOWN) {
            state = LogEntryState.KNOWN;
        }

        setView(view);

        this.value = value;

        onValueChange();
    }

    /**
     * Returns the value holding by this consensus. It represents last value
     * which was accepted by <code>Acceptor</code>.
     * 
     * @return the current value of this instance
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Gets the current state of this instance. When the state is set to
     * <code>DECIDED</code> no values should be changed.
     * 
     * @return current state of consensus instance
     */
    public LogEntryState getState() {
        return state;
    }

    /**
     * Gets the set of replicas from which we get the <code>ACCEPT</code>
     * message from the current <code>view</code>.
     * 
     * @return id's of replicas
     */
    public BitSet getAccepts() {
        return accepts;
    }

    /** Returns if the instances is accepted by the majority */
    public boolean isMajority() {
        return accepts.cardinality() >= ProcessDescriptor.processDescriptor.majority;
    }

    /**
     * Changes the current state of this instance to <code>DECIDED</code>. This
     * instance cannot be changed so <code>accepts</code> value will be set to
     * <code>null</code>.
     * 
     * @see #getAccepts()
     */
    public void setDecided() {
        assert value != null;
        state = LogEntryState.DECIDED;
        accepts = null;
        assertInvariant();
    }

    /**
     * Serializes and writes this consensus instance to specified byte buffer.
     * Specified byte buffer requires at least <code>byteSize()</code> remaining
     * size.
     * 
     * @param byteBuffer - the buffer where serialized consensus instance will
     *            be written
     * @see #byteSize()
     */
    public void write(ByteBuffer byteBuffer) {
        byteBuffer.putInt(id);
        byteBuffer.putInt(view);
        byteBuffer.putInt(state.ordinal());
        if (value == null) {
            byteBuffer.putInt(-1);
        } else {
            byteBuffer.putInt(value.length);
            byteBuffer.put(value);
        }
    }

    /**
     * Returns size of serialized instance in bytes. This value is equal to
     * length of array returned by <code>toByteArray()</code> method and number
     * of bytes written to <code>ByteBuffer</code> using
     * <code>write(ByteBuffer)</code> method.
     * 
     * @return size of serialized instance
     */
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
        result = prime * result + Arrays.hashCode(value);
        result = prime * result + view;
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ConsensusInstance other = (ConsensusInstance) obj;
        if (id != other.id) {
            return false;
        }
        if (state == null) {
            if (other.state != null) {
                return false;
            }
        } else if (!state.equals(other.state)) {
            return false;
        }
        if (!Arrays.equals(value, other.value)) {
            return false;
        }
        if (view != other.view) {
            return false;
        }
        return true;
    }

    public String toString() {
        return "(" + id + ", " + state + ", view=" + view + ", value=" + value + ")";
    }

    /** Called when received a higher view Accept */
    public void reset() {
        accepts.clear();
        state = LogEntryState.UNKNOWN;
        value = null;
        assertInvariant();
    }

    /**
     * Ignores any update with a view lower than the local one.
     * 
     * @param newView
     * @param newValue
     */
    public void updateStateFromKnown(int newView, byte[] newValue) {
        // Ignore any state update from an older view.
        if (newView < view) {
            return;
        }
        switch (state) {
            case DECIDED:
                /*
                 * This can happen when the new leader re-proposes an instance
                 * that was decided by some processes on a previous view.
                 * 
                 * The value must be the same as the local value.
                 */
                assert Arrays.equals(newValue, value) : "Values don't match. New view: " + newView +
                                                        ", local: " + this + ", newValue: " +
                                                        Arrays.toString(newValue) + ", old: " +
                                                        Arrays.toString(value);
                break;

            case KNOWN:
                // The message is for a higher view or same view and same value.
                // State remains in known
                assert newView != view || Arrays.equals(newValue, value) : "Values don't match. newView: " +
                                                                           newView +
                                                                           ", local: " +
                                                                           this;
                setValue(newView, newValue);
                break;

            case UNKNOWN:
                setValue(newView, newValue);
                break;

            default:
                throw new RuntimeException("Unknown instance state");
        }
    }

    /**
     * Update the local state from an already decided instance. This differs
     * from {@link #updateStateFromKnown(int, byte[])} in that it will accept
     * messages from lower views.
     * 
     * Used during catchup or view change, when the replica may receive messages
     * from lower views that are decided.
     * 
     * State is set to known, as the instance is decided from other class
     * 
     * This method does not check if args are valid, as this is unnecessary in
     * this case.
     * 
     * @param newView
     * @param newValue
     */
    public void updateStateFromDecision(int newView, byte[] newValue) {
        assert newValue != null;
        if (state == LogEntryState.DECIDED) {
            logger.warning("Updating a decided instance from a catchup message: " + this);
            // The value must be the same as the local value. No change.
            assert Arrays.equals(newValue, value) : "Values don't match. New view: " + newView +
                                                    ", local: " + this;
            return;
        } else {
            this.view = newView;
            this.value = newValue;
            this.state = LogEntryState.KNOWN;

            setDecidable();

            onValueChange();
        }
    }

    protected final void onValueChange() {
        if (value == null)
            return;
        for (ClientBatchID cbid : Batcher.unpack(value)) {
            ClientBatchStore.instance.associateWithInstance(cbid);
        }
    }

    /**
     * If the instance is ready to be decided, but misses batch values it is
     * decidable. Catch-Up will not bother for such instances.
     */
    public void setDecidable() {
        decidable = true;
    }

    /** Returns if the instance is decided or ready to be decided */
    public boolean isDecidable() {
        return LogEntryState.DECIDED.equals(state) || decidable;
    }

    private final static Logger logger = Logger.getLogger(ConsensusInstance.class.getCanonicalName());

}
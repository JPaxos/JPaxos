package lsr.paxos.storage;

import java.nio.ByteBuffer;
import java.util.Deque;

import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientBatchManager.FwdBatchRetransmitter;

public interface ConsensusInstance {

    /**
     * Represents possible states of consensus instance.
     * 
     * <pre>
     *                         ,---------.
     *                         |  RESET  |
     *                         '---------'
     *                    Accept ^     | 
     *                   (higher |     | Propose
     *                     view) |     V
     * ,---------.              ,---------.                ,---------.
     * | UNKNOWN |------------->|  KNOWN  |--------------->| DECIDED |
     * '---------'   Propose    '---------'  setDecided()  '---------'
     *   \    ^                   \    ^
     *    \__/                     \__/
     *   Accept                   Accept
     *                         (same view)
     * </pre>
     */
    public enum LogEntryState {
    /**
     * state when no value is known.
     */
    UNKNOWN,
    /**
     * state when the view and value is known, and the process sent an
     * accept/propose
     */
    KNOWN,
    /**
     * state when <code>PROPOSE</code> was received for an older view and an
     * <code>ACCEPT</code> was received for a more recent view. Old value and
     * view are available, but the current value is not known.
     */
    RESET,
    /**
     * state after setDecided was called, usually once a majority of
     * <code>ACCEPT</code>s were received.
     */
    DECIDED
    }

    /**
     * Gets the number of the consensus instance. Different instances should
     * have different id's.
     * 
     * @return id of instance
     */
    int getId();

    /**
     * Gets the current view of this instance. The view of instance is
     * represented by the view of last message.
     * 
     * @return the current view number of this instance
     */
    int getLastSeenView();

    /**
     * Gets the view in which last Accept or Propose for this instance was sent.
     * 
     * @return the view number of last vote
     */
    int getLastVotedView();

    /**
     * Returns the value holding by this consensus. It represents last value
     * which was accepted by <code>Acceptor</code>.
     * 
     * @return the current value of this instance
     */
    byte[] getValue();

    /**
     * Gets the current state of this instance. When the state is set to
     * <code>DECIDED</code> no values should be changed.
     * 
     * @return current state of consensus instance
     */
    LogEntryState getState();

    /** Returns if the instances is accepted by the majority */
    boolean isMajority();

    /**
     * Changes the current state of this instance to <code>DECIDED</code>. This
     * is irreversible.
     */
    void setDecided();

    /**
     * Serializes and writes this consensus instance to specified byte buffer.
     * Specified byte buffer requires at least <code>byteSize()</code> remaining
     * size. The view and the value of the instance is taken from the last vote,
     * which matters iff the instance is RESET.
     * 
     * @param byteBuffer - the buffer where serialized consensus instance will
     *            be written
     * @see #byteSize()
     */
    void writeAsLastVoted(ByteBuffer byteBuffer);

    /**
     * Returns size of serialized instance in bytes. This value is equal to the
     * number of bytes written to <code>ByteBuffer</code> using
     * <code>writeAsLastVoted(ByteBuffer)</code> method.
     * 
     * @return size of serialized instance
     */
    int byteSize();

    String toString();

    /**
     * Called upon a <code>Propose</code>. The Propose must be in no lesser view
     * than the current. Instance must not be decided. Returns whether the
     * instance can be decided.
     * 
     * Called both when a Propose is send (with sender == localid) and when a
     * Propose is received.
     * 
     * @param sender - the sender of the Propose
     * @param newView - the view of the Propose
     * @param newValue - the value sent in the Propose
     * 
     * @return true if can decide the instance
     */
    boolean updateStateFromPropose(int sender, int newView, byte[] newValue);

    /**
     * Called upon an <code>Accept</code>. The Accept must be in no lesser view
     * than the current. Instance must not be decided. Returns whether the
     * instance can be decided.
     * 
     * @param view - the view of the Accept
     * @param acceptSender - id of the replica that sent the accept
     * @return true iff there is a majority and a value
     */
    boolean updateStateFromAccept(int view, int acceptSender);

    /**
     * Unchecked setter of the view and value. Should be called iff the presence
     * of an already decided instance is known. Used during catchup or view
     * change, when the replica may receive messages from lower views that are
     * decided. This method does not check if args are valid, as this is
     * unnecessary in this case.
     * 
     * State is set to known, as decide must be called by learner to continue
     * processing of the value.
     * 
     * @param newView
     * @param newValue
     */
    void updateStateFromDecision(int newView, byte[] newValue);

    /* - - - - - INDIRECT CONSENSUS - - - - - */

    /**
     * If the instance is ready to be decided, but misses batch values it is
     * decidable. Catch-Up will not bother for such instances.
     */
    void setDecidable(boolean decidable);

    /**
     * Returns if the instance is decided or ready to be decided.
     * 
     * FOR CATCH-UP PURPOSES ONLY
     */
    boolean isDecidable();

    /**
     * Returns the batch IDs contained in the request, unpacking the request if
     * necessary
     * 
     * DO NOT MODIFY THE RESULT
     */
    Deque<ClientBatchID> getClientBatchIds();

    /**
     * If it has been necessary to unpack the value earlier, this allows not to
     * waste the effort of unpacking
     */
    void setClientBatchIds(Deque<ClientBatchID> cbids);

    /** If there is a task fetching missing batch values, the task is stopped */
    void stopFwdBatchForwarder();

    /** Sets a task for fetching the batch values */
    void setFwdBatchForwarder(FwdBatchRetransmitter fbr);

}
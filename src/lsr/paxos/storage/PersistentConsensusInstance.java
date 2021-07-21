package lsr.paxos.storage;

import java.lang.annotation.Native;
import java.nio.ByteBuffer;
import java.util.Deque;

import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientBatchManager.FwdBatchRetransmitter;

public class PersistentConsensusInstance implements ConsensusInstance {

    private int id;

    public PersistentConsensusInstance(int id) {
        this.id = id;
    }

    @Override
    public int getId() {
        return id;
    }

    private static native int getLastSeenView(int id);

    private static native int getLastVotedView(int id);

    @Override
    public int getLastSeenView() {
        return getLastSeenView(id);
    }

    @Override
    public int getLastVotedView() {
        return getLastVotedView(id);
    }

    private static native byte[] getValue(int id);

    @Override
    public byte[] getValue() {
        return getValue(id);
    }

    @Native
    private static final byte ENUM_LOGENTRYSTATE_UNKNOWN = 0;
    @Native
    private static final byte ENUM_LOGENTRYSTATE_KNOWN = 1;
    @Native
    private static final byte ENUM_LOGENTRYSTATE_RESET = 2;
    @Native
    private static final byte ENUM_LOGENTRYSTATE_DECIDED = 3;

    private static native byte getState(int id);

    @Override
    public LogEntryState getState() {
        switch (getState(id)) {
            case ENUM_LOGENTRYSTATE_UNKNOWN:
                return LogEntryState.UNKNOWN;
            case ENUM_LOGENTRYSTATE_KNOWN:
                return LogEntryState.KNOWN;
            case ENUM_LOGENTRYSTATE_RESET:
                return LogEntryState.RESET;
            case ENUM_LOGENTRYSTATE_DECIDED:
                return LogEntryState.DECIDED;
            default:
                throw new IllegalArgumentException();
        }
    }

    private native static boolean isMajority(int id);

    @Override
    public boolean isMajority() {
        return isMajority(id);
    }

    private native static void setDecided(int id);

    @Override
    public void setDecided() {
        setDecided(id);
    }

    private native static int writeAsLastVoted(int id, ByteBuffer bb, int pos);

    private native static byte[] writeAsLastVoted(int id);

    @Override
    public void writeAsLastVoted(ByteBuffer byteBuffer) {
        if (byteBuffer.isDirect()) {
            int written = writeAsLastVoted(id, byteBuffer, byteBuffer.position());
            byteBuffer.position(byteBuffer.position() + written);
        } else {
            byteBuffer.put(writeAsLastVoted(id));
        }
    }

    private native static int byteSize(int id);

    @Override
    public int byteSize() {
        return byteSize(id);
    }

    private static native boolean updateStateFromAccept(int id, int view, int acceptSender);

    @Override
    public boolean updateStateFromAccept(int view, int acceptSender) {
        return updateStateFromAccept(id, view, acceptSender);
    }

    private static native boolean updateStateFromPropose(int id, int sender, int newView,
                                                         byte[] newValue);

    @Override
    public boolean updateStateFromPropose(int sender, int newView, byte[] newValue) {
        return updateStateFromPropose(id, sender, newView, newValue);
    }

    public static native void updateStateFromDecision(int id, int newView, byte[] newValue);

    @Override
    public void updateStateFromDecision(int newView, byte[] newValue) {
        updateStateFromDecision(id, newView, newValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof PersistentConsensusInstance))
            return false;
        PersistentConsensusInstance other = (PersistentConsensusInstance) obj;
        return this.id == other.id;
    }

    @Override
    public String toString() {
        return "PerInst:" + id;
    }

    /* - - - - - INDIRECT CONSENSUS - - - - - */

    @Override
    public void setDecidable(boolean decidable) {
        throw new UnsupportedOperationException("Indirect consensus is unsupported with pmem yet");
    }

    @Override
    public boolean isDecidable() {
        return LogEntryState.DECIDED.equals(getState());
    }

    @Override
    public Deque<ClientBatchID> getClientBatchIds() {
        throw new UnsupportedOperationException("Indirect consensus is unsupported with pmem yet");
    }

    @Override
    public void setClientBatchIds(Deque<ClientBatchID> cbids) {
        throw new UnsupportedOperationException("Indirect consensus is unsupported with pmem yet");
    }

    @Override
    public void stopFwdBatchForwarder() {
        throw new UnsupportedOperationException("Indirect consensus is unsupported with pmem yet");
    }

    @Override
    public void setFwdBatchForwarder(FwdBatchRetransmitter fbr) {
        throw new UnsupportedOperationException("Indirect consensus is unsupported with pmem yet");
    }
}

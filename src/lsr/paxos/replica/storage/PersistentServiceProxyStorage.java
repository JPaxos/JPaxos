package lsr.paxos.replica.storage;

import java.util.List;

import lsr.common.Pair;
import lsr.common.Reply;
import lsr.paxos.Snapshot;

public class PersistentServiceProxyStorage implements ServiceProxyStorage {

    /*-     Not persistent part (aux data used to restore from snapshot)    -*/

    private List<Reply> partialResponseCache;
    private int skip;

    @Override
    public byte[] getNextSkippedRequestResponse() {
        return partialResponseCache.remove(0).getValue();
    }

    @Override
    public void setSkippedCache(List<Reply> partialResponseCache) {
        this.partialResponseCache = partialResponseCache;
    }

    @Override
    public void setSkip(int skip) {
        this.skip = skip;
    }

    @Override
    public boolean decSkip() {
        if (skip == 0)
            return false;
        skip--;
        return true;
    }

    @Override
    public boolean hasSkip() {
        return skip != 0;
    }

    /*-    Persistent part    -*/

    @Override
    public native void incNextSeqNo();

    @Override
    public native int getNextSeqNo();

    @Override
    public native void setNextSeqNo(int seqNo);

    @Override
    public native int getLastSnapshotNextSeqNo();

    @Override
    public native void setLastSnapshotNextSeqNo(int lssn);

    @Override
    public native void addStartingSeqenceNo(int instance, int sequenceNo);

    @Override
    public native void truncateStartingSeqNo(int lowestSeqNo);

    @Override
    public native void clearStartingSeqNo();

    private native int[] getFrontStartingSeqNo_();

    @Override
    public Pair<Integer, Integer> getFrontStartingSeqNo() {
        int[] pair = getFrontStartingSeqNo_();
        return new Pair<Integer, Integer>(pair[0], pair[1]);
    }

    @Override
    public void onSnapshotMade(Snapshot snapshot) {
    }
}

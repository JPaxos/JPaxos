package lsr.paxos.replica.storage;

import lsr.common.Pair;
import lsr.paxos.Snapshot;

public class SnapshotlyPersistentServiceProxyStorage extends PersistentServiceProxyStorage {

    int nextSeqNo;

    public SnapshotlyPersistentServiceProxyStorage(Snapshot lastSnapshot) {
        super();
        if (lastSnapshot != null) {
            addStartingSeqenceNo(lastSnapshot.getNextInstanceId(),
                    lastSnapshot.getStartingRequestSeqNo());
            nextSeqNo = lastSnapshot.getStartingRequestSeqNo();
        } else
            addStartingSeqenceNo(0, 0);
    }

    @Override
    public void incNextSeqNo() {
        nextSeqNo++;
    }

    @Override
    public int getNextSeqNo() {
        return nextSeqNo;
    }

    @Override
    public void setNextSeqNo(int seqNo) {
        nextSeqNo = seqNo;
    }

    InMemoryServiceProxyStorage imsps = new InMemoryServiceProxyStorage();

    @Override
    public void addStartingSeqenceNo(int instance, int sequenceNo) {
        imsps.addStartingSeqenceNo(instance, sequenceNo);
    };

    @Override
    public void truncateStartingSeqNo(int lowestSeqNo) {
        imsps.truncateStartingSeqNo(lowestSeqNo);
    }

    @Override
    public void clearStartingSeqNo() {
        imsps.clearStartingSeqNo();
    }

    @Override
    public Pair<Integer, Integer> getFrontStartingSeqNo() {
        return imsps.getFrontStartingSeqNo();
    }

    @Override
    public void onSnapshotMade(Snapshot snapshot) {
        super.onSnapshotMade(snapshot);
        setLastSnapshotNextSeqNo(snapshot.getNextRequestSeqNo());
    }

}

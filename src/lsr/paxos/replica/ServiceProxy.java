package lsr.paxos.replica;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Vector;

import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Snapshot;
import lsr.service.Service;

public class ServiceProxy implements SnapshotListener {

    /** descending sorted map of RequestSeqNo starting each instance */
    private NavigableMap<Integer, Integer> startingSeqNo = new TreeMap<Integer, Integer>().descendingMap();
    {
        startingSeqNo.put(0, 0);
    }

    /** Next RequestSeqNo to be passed on */
    private int nextSeqNo = 0;

    /** RequestSeqNo for the last snapshot */
    private int lastSnapshotNextSeqNo = -1;

    /** Used only by recovery, describes how many requests on should be skipped */
    private int skip = 0;
    /** Used only by recovery, holds responses for skipped requests */
    List<Reply> skippedCache;

    /** Used for keeping requestId for snapshot purposes. */
    private Request currentRequest;

    private final Service service;
    Vector<SnapshotListener2> listeners = new Vector<SnapshotListener2>();
    private final Map<Integer, List<Reply>> responsesCache;

    private final SingleThreadDispatcher replicaDispatcher;

    public ServiceProxy(Service service, Map<Integer, List<Reply>> responsesCache,
                        SingleThreadDispatcher replicaDispatcher) {
        this.service = service;
        this.replicaDispatcher = replicaDispatcher;
        service.addSnapshotListener(this);
        this.responsesCache = responsesCache;
    }

    public final byte[] execute(Request request, int instanceId) {
        nextSeqNo++;
        if (skip > 0) {
            skip--;
            assert !skippedCache.isEmpty();
            return skippedCache.remove(0).getValue();
        } else {
            currentRequest = request;
            return service.execute(request.getValue(), nextSeqNo - 1);
        }
    }

    public final void instanceExecuted(int instanceId) {
        startingSeqNo.put(instanceId + 1, nextSeqNo);
    }

    public void askForSnapshot(int lastSnapshotInstance) {
        service.askForSnapshot(lastSnapshotNextSeqNo);
    }

    public void forceSnapshot(int lastSnapshotInstance) {
        service.forceSnapshot(lastSnapshotNextSeqNo);
    }

    public void updateToSnapshot(Snapshot snapshot) {
        lastSnapshotNextSeqNo = snapshot.nextRequestSeqNo;
        nextSeqNo = snapshot.startingRequestSeqNo;
        skip = snapshot.nextRequestSeqNo - nextSeqNo;

        skippedCache = snapshot.partialResponseCache;

        startingSeqNo.put(snapshot.nextIntanceId, snapshot.startingRequestSeqNo);

        service.updateToSnapshot(lastSnapshotNextSeqNo, snapshot.value);
    }

    public void onSnapshotMade(final int nextRequestSeqNo, final byte[] value,
                               final byte[] response) {
        replicaDispatcher.executeAndWait(new Runnable() {
            public void run() {
                if (value == null)
                    throw new IllegalArgumentException("The snapshot value cannot be null");
                if (nextRequestSeqNo < lastSnapshotNextSeqNo)
                    throw new IllegalArgumentException("The snapshot is older than previous");
                if (nextRequestSeqNo > nextSeqNo)
                    throw new IllegalArgumentException(
                            "The snapshot marked as newer than current state");

                Entry<Integer, Integer> nextInstanceEntry = startingSeqNo.firstEntry();

                for (Map.Entry<Integer, Integer> entry : startingSeqNo.entrySet()) {
                    assert nextInstanceEntry.getKey() >= entry.getKey();
                    // the map is sorted descending
                    if (entry.getValue() < nextRequestSeqNo)
                        break;
                    nextInstanceEntry = entry;
                }

                Snapshot snapshot = new Snapshot();

                snapshot.nextRequestSeqNo = nextRequestSeqNo;
                snapshot.nextIntanceId = nextInstanceEntry.getKey();
                snapshot.startingRequestSeqNo = nextInstanceEntry.getValue();
                snapshot.value = value;

                int localSkip = snapshot.nextRequestSeqNo - snapshot.startingRequestSeqNo;
                List<Reply> thisInstanceReplies = responsesCache.get(snapshot.nextIntanceId);
                if (thisInstanceReplies == null) {
                    assert snapshot.startingRequestSeqNo == nextSeqNo;
                    snapshot.partialResponseCache = new Vector<Reply>(0);
                } else {
                    snapshot.partialResponseCache = new Vector<Reply>(thisInstanceReplies.subList(
                            0, localSkip));

                    boolean hasLastResponse = localSkip == 0 ||
                                              snapshot.partialResponseCache.get(localSkip - 1) != null;
                    if (!hasLastResponse) {
                        if (response == null)
                            throw new IllegalArgumentException(
                                    "If snapshot is executed from within execute()"
                                            + " for current request, the response has to be given with snapshot");
                        snapshot.partialResponseCache.add(new Reply(currentRequest.getRequestId(),
                                response));
                    }
                }

                lastSnapshotNextSeqNo = nextRequestSeqNo;

                for (SnapshotListener2 listener : listeners) {
                    listener.onSnapshotMade(snapshot);
                }
            }
        });
    }

    public Integer getLastSnapshotSeqNo() {
        return lastSnapshotNextSeqNo;
    }

    public void recoveryFinished() {
        service.recoveryFinished();
    }

    public void addSnapshotListener(SnapshotListener2 replica) {
        listeners.add(replica);
    }

}

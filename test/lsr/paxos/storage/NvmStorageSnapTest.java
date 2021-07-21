package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.TreeMap;

import org.junit.Test;

import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.NvmTestTemplate;
import lsr.paxos.Snapshot;

public class NvmStorageSnapTest extends NvmTestTemplate {

    PersistentStorage storage = new PersistentStorage();
    PersistentLog log = storage.getLog();

    byte[] sv1 = {'a', 'b', 'c', 'd'};
    byte[] sv2 = {'e', 'f', 'g', 'h', 'i'};
    Snapshot snapshot1;

    {
        TreeMap<Long, Reply> crm = new TreeMap<>();
        crm.put(2L, new Reply(new RequestId(2, 1), new byte[] {'2', '1'}));
        crm.put(5L, new Reply(new RequestId(5, 3), new byte[] {'5', '3', '-'}));

        ArrayList<Reply> prc = new ArrayList<>();
        prc.add(new Reply(new RequestId(2, 2), new byte[] {'o'}));
        prc.add(new Reply(new RequestId(7, 0), new byte[] {'a', 'b', 'c', 'd'}));

        snapshot1 = new Snapshot();
        snapshot1.setValue(sv1);
        snapshot1.setNextInstanceId(5);
        snapshot1.setStartingRequestSeqNo(40);
        snapshot1.setNextRequestSeqNo(42);
        snapshot1.setPartialResponseCache(prc);
        snapshot1.setLastReplyForClient(crm);
    }

    @Test
    public void snapshots() {

        assertEquals(null, storage.getLastSnapshotNextId());
        assertEquals(null, storage.getLastSnapshot());

        storage.setLastSnapshot(snapshot1);

        assertEquals(Integer.valueOf(5), storage.getLastSnapshotNextId());
        assertEquals(snapshot1, storage.getLastSnapshot());
    }
}

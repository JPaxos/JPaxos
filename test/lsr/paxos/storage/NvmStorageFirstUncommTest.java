package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.TreeMap;

import org.junit.Test;

import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.NvmTestTemplate;
import lsr.paxos.Snapshot;

public class NvmStorageFirstUncommTest extends NvmTestTemplate {

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
    public void firstUncommited() {
        assertEquals(0, storage.getFirstUncommitted());
        storage.updateFirstUncommitted();
        assertEquals(0, storage.getFirstUncommitted());
        PersistentConsensusInstance ci0 = log.append();
        PersistentConsensusInstance ci1 = log.append();
        PersistentConsensusInstance ci2 = log.append();
        PersistentConsensusInstance ci3 = log.append();

        ci0.updateStateFromPropose(0, 5, new byte[] {'a'});
        ci0.updateStateFromAccept(5, 2);

        ci1.updateStateFromPropose(0, 5, new byte[] {'b'});
        ci1.updateStateFromAccept(5, 2);

        ci3.updateStateFromPropose(0, 5, new byte[] {'c'});
        ci3.updateStateFromAccept(5, 2);

        storage.updateFirstUncommitted();
        assertEquals(0, storage.getFirstUncommitted());

        ci0.setDecided();

        storage.updateFirstUncommitted();
        assertEquals(1, storage.getFirstUncommitted());

        ci2.setDecided();

        storage.updateFirstUncommitted();
        assertEquals(1, storage.getFirstUncommitted());

        ci1.setDecided();

        storage.updateFirstUncommitted();
        assertEquals(3, storage.getFirstUncommitted());

        storage.setLastSnapshot(snapshot1);

        storage.updateFirstUncommitted();
        assertEquals(5, storage.getFirstUncommitted());

        PersistentConsensusInstance ci5 = log.getInstance(5);
        ci5.updateStateFromDecision(10, new byte[] {'f'});
        ci5.setDecided();

        storage.updateFirstUncommitted();
        assertEquals(6, storage.getFirstUncommitted());
    }
}

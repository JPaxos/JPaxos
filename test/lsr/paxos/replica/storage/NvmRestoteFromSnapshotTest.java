package lsr.paxos.replica.storage;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.NvmTestTemplate;
import lsr.paxos.Snapshot;
import lsr.paxos.storage.PersistentConsensusInstance;

public class NvmRestoteFromSnapshotTest extends NvmTestTemplate {
    public PersistentReplicaStorage storage = new PersistentReplicaStorage();

    Snapshot snapshot;
    Reply reply03 = new Reply(new RequestId(3, 0), new byte[] {'a', 'b', 'c'});
    Reply reply04 = new Reply(new RequestId(4, 0), new byte[] {'d', 'e'});
    Reply reply13 = new Reply(new RequestId(3, 1), new byte[] {'f', 'g', 'h', 'i'});
    Reply reply15 = new Reply(new RequestId(5, 0), new byte[] {'j', 'k', 'l'});
    Reply reply25 = new Reply(new RequestId(5, 1), new byte[] {'m', 'n'});
    Reply reply33 = new Reply(new RequestId(3, 2), new byte[] {'o', 'p', 'q'});
    Reply reply35 = new Reply(new RequestId(5, 2), new byte[0]);
    Reply reply06 = new Reply(new RequestId(6, 0), new byte[] {'r', 's', 't'});
    {
        snapshot = new Snapshot();

        HashMap<Long, Reply> rlfc = new HashMap<Long, Reply>();
        rlfc.put(3l, reply13);
        rlfc.put(4l, reply04);
        rlfc.put(5l, reply25);

        snapshot.setLastReplyForClient(rlfc);

        snapshot.setNextInstanceId(4);

        ArrayList<Reply> prc = new ArrayList<Reply>();
        prc.add(reply33);
        snapshot.setPartialResponseCache(prc);

        snapshot.setNextRequestSeqNo(7);

        snapshot.setStartingRequestSeqNo(6);
    }

    @Test
    public void simple() {
        storage.addDecidedWaitingExecution(0, new PersistentConsensusInstance(0));
        storage.getDecidedWaitingExecution(0);
        storage.setLastReplyForClient(0, 3l, reply03);
        storage.setLastReplyForClient(0, 4l, reply04);
        storage.incrementExecuteUB();

        storage.addDecidedWaitingExecution(1, new PersistentConsensusInstance(1));
        storage.getDecidedWaitingExecution(1);
        storage.setLastReplyForClient(1, 5l, reply15);
        storage.incrementExecuteUB();

        Map<Long, Reply> ruiReal = storage.getLastRepliesUptoInstance(1);

        storage.addDecidedWaitingExecution(2, new PersistentConsensusInstance(2));

        HashMap<Long, Reply> ruiExp = new HashMap<Long, Reply>();
        ruiExp.put(3l, reply03);
        ruiExp.put(4l, reply04);

        assertEquals(reply03, storage.getLastReplyForClient(3l));
        assertEquals(reply15, storage.getLastReplyForClient(5l));
        assertEquals(ruiExp, ruiReal);
        assertEquals(2, storage.getExecuteUB());

        /*
         * 
         */

        storage.restoreFromSnapshot(snapshot);

        /*
         * 
         */

        assertEquals(null, storage.getDecidedWaitingExecution(2));
        assertEquals(4, storage.getExecuteUB());
        assertEquals(reply13, storage.getLastReplyForClient(3l));
        assertEquals(reply25, storage.getLastReplyForClient(5l));

        storage.addDecidedWaitingExecution(4, new PersistentConsensusInstance(4));
        for (Reply r : snapshot.getPartialResponseCache()) {
            storage.setLastReplyForClient(snapshot.getNextInstanceId(),
                    r.getRequestId().getClientId(), r);
        }

        storage.setLastReplyForClient(4, 6l, reply06);
        storage.incrementExecuteUB();

        Map<Long, Reply> rui2Real = storage.getLastRepliesUptoInstance(5);
        HashMap<Long, Reply> rui2Exp = new HashMap<Long, Reply>();
        rui2Exp.put(3l, reply33);
        rui2Exp.put(4l, reply04);
        rui2Exp.put(5l, reply25);
        rui2Exp.put(6l, reply06);

        assertEquals(rui2Exp, rui2Real);

    }
}

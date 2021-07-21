package lsr.paxos.replica.storage;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.NvmTestTemplate;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NvmClientReplyTest extends NvmTestTemplate {
    public PersistentReplicaStorage storage = new PersistentReplicaStorage();

    Reply reply03 = new Reply(new RequestId(3, 0), new byte[] {'a', 'b', 'c'});
    Reply reply04 = new Reply(new RequestId(4, 0), new byte[] {'d', 'e'});
    Reply reply13 = new Reply(new RequestId(3, 1), new byte[] {'f', 'g', 'h', 'i'});
    Reply reply15 = new Reply(new RequestId(5, 0), new byte[] {'j', 'k', 'l'});
    Reply reply25 = new Reply(new RequestId(5, 1), new byte[] {'m', 'n'});
    Reply reply35 = new Reply(new RequestId(5, 2), new byte[0]);
    Reply reply45 = new Reply(new RequestId(5, 3), new byte[] {'r', 's', 't'});

    @Test
    public void a_inital() {
        assertEquals(null, storage.getLastReplySeqNoForClient(1l));
        assertEquals(null, storage.getLastReplyForClient(2l));
    }

    @Test
    public void b_getSet() {
        storage.setLastReplyForClient(0, 4l, reply04);
        storage.setLastReplyForClient(0, 3l, reply03);

        assertEquals(reply03, storage.getLastReplyForClient(3l));
        assertEquals(reply04, storage.getLastReplyForClient(4l));

        assertEquals(Integer.valueOf(0), storage.getLastReplySeqNoForClient(3l));
        assertEquals(Integer.valueOf(0), storage.getLastReplySeqNoForClient(4l));

        storage.setLastReplyForClient(1, 3l, reply13);
        storage.setLastReplyForClient(1, 5l, reply15);

        assertEquals(reply13, storage.getLastReplyForClient(3l));
        assertEquals(reply04, storage.getLastReplyForClient(4l));
        assertEquals(reply15, storage.getLastReplyForClient(5l));

        assertEquals(Integer.valueOf(1), storage.getLastReplySeqNoForClient(3l));
        assertEquals(Integer.valueOf(0), storage.getLastReplySeqNoForClient(4l));
        assertEquals(Integer.valueOf(0), storage.getLastReplySeqNoForClient(5l));
    }

    @Test
    public void c_repliesUptoInst() {
        storage.setLastReplyForClient(2, 5l, reply25);
        storage.setLastReplyForClient(3, 5l, reply35);
        storage.setLastReplyForClient(4, 5l, reply45);

        Map<Long, Reply> retval1 = storage.getLastRepliesUptoInstance(2);

        HashMap<Long, Reply> expected = new HashMap<Long, Reply>();
        expected.put(3l, reply13);
        expected.put(4l, reply04);
        expected.put(5l, reply15);

        assertEquals(expected, retval1);

        Map<Long, Reply> retval2 = storage.getLastRepliesUptoInstance(4);
        expected.put(5l, reply35);

        assertEquals(expected, retval2);
    }

}

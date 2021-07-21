package lsr.paxos.replica.storage;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.NvmCommonSetup;

public class NvmClientReplyPartATest {
    public PersistentReplicaStorage storage = new PersistentReplicaStorage();

    final static Reply reply1 = new Reply(new RequestId(1, 0), new byte[] {'a', 'b', 'c'});
    final static Reply reply2 = new Reply(new RequestId(2, 0), new byte[] {'d', 'e'});
    final static Reply reply3 = new Reply(new RequestId(3, 1), new byte[] {'f', 'g', 'h', 'i'});
    final static Reply reply4 = new Reply(new RequestId(4, 1), new byte[] {'j', 'k', 'l'});
    final static Reply reply5 = new Reply(new RequestId(5, 2), new byte[] {'m', 'n'});
    final static Reply reply6 = new Reply(new RequestId(6, 2), new byte[0]);
    final static Reply reply7 = new Reply(new RequestId(7, 3), new byte[] {'r', 's', 't'});

    @BeforeClass
    public static void setup() throws Throwable {
        NvmCommonSetup.initProcessDescr(5, 1);
        NvmCommonSetup.loadPmem();
    }

    @Test
    public void partA() {
        storage.setLastReplyForClient(0, 1l, reply1);
        storage.setLastReplyForClient(0, 3l, new Reply(new RequestId(3, 0), new byte[] {'1'}));
        storage.setLastReplyForClient(0, 4l, new Reply(new RequestId(4, 0), new byte[] {'2', '3'}));
        storage.setLastReplyForClient(0, 5l,
                new Reply(new RequestId(5, 0), new byte[] {'4', '1', '2'}));
        storage.setLastReplyForClient(0, 6l,
                new Reply(new RequestId(6, 0), new byte[] {'4', '1', '2'}));

        storage.setLastReplyForClient(1, 5l, new Reply(new RequestId(5, 1), new byte[] {}));
        storage.setLastReplyForClient(1, 2l, reply2);
        storage.setLastReplyForClient(1, 6l, new Reply(new RequestId(6, 1), new byte[] {'6', '7'}));
        storage.setLastReplyForClient(1, 7l,
                new Reply(new RequestId(7, 0), new byte[] {'8', '2', '2'}));

        storage.setLastReplyForClient(2, 7l,
                new Reply(new RequestId(7, 1), new byte[] {'9', '7', '4', '3'}));
        storage.setLastReplyForClient(2, 3l, reply3);
        storage.setLastReplyForClient(3, 8l, new Reply(new RequestId(8, 0), new byte[0]));

        storage.setLastReplyForClient(3, 4l, reply4);
        storage.setLastReplyForClient(3, 7l, new Reply(new RequestId(7, 2), new byte[] {'0', '2'}));
        storage.setLastReplyForClient(3, 5l, reply5);
        storage.setLastReplyForClient(3, 9l,
                new Reply(new RequestId(9, 0), new byte[] {'4', '3', '6'}));

        storage.setLastReplyForClient(4, 6l, reply6);
        storage.setLastReplyForClient(4, 7l, reply7);

        check(storage);
    }

    public static void check(PersistentReplicaStorage storage) {
        assertEquals(reply1, storage.getLastReplyForClient(1l));
        assertEquals(reply2, storage.getLastReplyForClient(2l));
        assertEquals(reply3, storage.getLastReplyForClient(3l));
        assertEquals(reply4, storage.getLastReplyForClient(4l));
        assertEquals(reply5, storage.getLastReplyForClient(5l));
        assertEquals(reply6, storage.getLastReplyForClient(6l));
        assertEquals(reply7, storage.getLastReplyForClient(7l));
    }
}

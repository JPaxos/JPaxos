package lsr.paxos.replica.storage;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import lsr.common.Pair;
import lsr.paxos.NvmTestTemplate;

public class NvmServiceProxyStorageTest extends NvmTestTemplate {
    PersistentServiceProxyStorage storage = new PersistentServiceProxyStorage();
    
    @Test
    public void simplesTest() {
        assertEquals(0, storage.getNextSeqNo());
        storage.incNextSeqNo();
        assertEquals(1, storage.getNextSeqNo());
        storage.incNextSeqNo();
        assertEquals(2, storage.getNextSeqNo());
        storage.setNextSeqNo(5);
        assertEquals(5, storage.getNextSeqNo());
        
        assertEquals(0, storage.getLastSnapshotNextSeqNo());
        storage.setLastSnapshotNextSeqNo(10);
        assertEquals(10, storage.getLastSnapshotNextSeqNo());
    }
    
    @Test
    public void startingSeqNoTest() {
        assertEquals(new Pair<Integer, Integer>(0,0), storage.getFrontStartingSeqNo());
        
        storage.truncateStartingSeqNo(5);
        assertEquals(new Pair<Integer, Integer>(0,0), storage.getFrontStartingSeqNo());

        storage.addStartingSeqenceNo(2, 4);
        
        storage.truncateStartingSeqNo(5);
        assertEquals(new Pair<Integer, Integer>(2,4), storage.getFrontStartingSeqNo());

        storage.addStartingSeqenceNo(4, 8);
        storage.addStartingSeqenceNo(8, 16);
        storage.addStartingSeqenceNo(10, 20);
        storage.addStartingSeqenceNo(12, 24);
        
        storage.truncateStartingSeqNo(15);
        assertEquals(new Pair<Integer, Integer>(4,8), storage.getFrontStartingSeqNo());
        
        storage.truncateStartingSeqNo(15);
        assertEquals(new Pair<Integer, Integer>(4,8), storage.getFrontStartingSeqNo());
        
        storage.truncateStartingSeqNo(16);
        assertEquals(new Pair<Integer, Integer>(8,16), storage.getFrontStartingSeqNo());
        
        storage.truncateStartingSeqNo(25);
        assertEquals(new Pair<Integer, Integer>(12,24), storage.getFrontStartingSeqNo());
        
        storage.clearStartingSeqNo();
        storage.addStartingSeqenceNo(20, 40);
        assertEquals(new Pair<Integer, Integer>(20,40), storage.getFrontStartingSeqNo());
    }
}

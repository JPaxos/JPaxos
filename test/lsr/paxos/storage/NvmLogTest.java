package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;

import java.util.TreeMap;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import lsr.paxos.NvmTestTemplate;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NvmLogTest extends NvmTestTemplate {
    PersistentStorage storage = new PersistentStorage();
    PersistentLog log = storage.getLog();
    
    @Test
    public void a_emptyLog() {
        assertEquals(0, log.getLowestAvailableId());
        assertEquals(0, log.getNextId());
        assertEquals(0, log.byteSizeBetween(5, 10));
    }
    
    
    @Test
    public void b_append() {
        TreeMap<Integer, PersistentConsensusInstance> expected = new TreeMap<>();
        PersistentConsensusInstance ci0 = log.append();
        PersistentConsensusInstance ci1 = log.append();
        PersistentConsensusInstance ci2 = log.append();
        
        assertEquals(0, log.getLowestAvailableId());
        assertEquals(3, log.getNextId());
        
        expected.put(0, ci0);
        expected.put(1, ci1);
        expected.put(2, ci2);
        
        assertEquals(expected, log.getInstanceMap());
    }
    
    @Test
    public void c_getInstance() {
        TreeMap<Integer, PersistentConsensusInstance> expected = new TreeMap<>();
        PersistentConsensusInstance ci5 = log.getInstance(5);
        
        assertEquals(0, log.getLowestAvailableId());
        assertEquals(6, log.getNextId());

        expected.put(0, new PersistentConsensusInstance(0));
        expected.put(1, new PersistentConsensusInstance(1));
        expected.put(2, new PersistentConsensusInstance(2));
        expected.put(3, new PersistentConsensusInstance(3));
        expected.put(4, new PersistentConsensusInstance(4));
        expected.put(5, ci5);
        
        assertEquals(expected, log.getInstanceMap());
    }
    
    
    
    
    
}
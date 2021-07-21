package lsr.paxos.replica.storage;

import static org.junit.Assert.assertEquals;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import lsr.paxos.NvmTestTemplate;
import lsr.paxos.storage.PersistentConsensusInstance;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NvmDecidedWaitingExecTest extends NvmTestTemplate {
    public PersistentReplicaStorage storage = new PersistentReplicaStorage();
    
    @Test
    public void a_initial() {
       assertEquals(0, storage.getExecuteUB());
       storage.setExecuteUB(2); 
    }
    
    @Test
    public void b_setAndGet() {
       storage.setExecuteUB(2); 
       assertEquals(2, storage.getExecuteUB());
    }
    
    @Test
    public void c_increment() {
       storage.setExecuteUB(5);
       storage.incrementExecuteUB();
       storage.incrementExecuteUB();
       assertEquals(7, storage.getExecuteUB());
    }
    
    @Test
    public void decWaitingEcecTest() {
        storage.addDecidedWaitingExecution(0, null);
        storage.addDecidedWaitingExecution(1, null);
        storage.addDecidedWaitingExecution(2, null);
       
        assertEquals(new PersistentConsensusInstance(0), storage.getDecidedWaitingExecution(0));
        assertEquals(new PersistentConsensusInstance(1), storage.getDecidedWaitingExecution(1));
        assertEquals(new PersistentConsensusInstance(2), storage.getDecidedWaitingExecution(2));
        assertEquals(null, storage.getDecidedWaitingExecution(3));
        
        assertEquals(3, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecution(0);
        assertEquals(2, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecution(1);
        assertEquals(1, storage.decidedWaitingExecutionCount());
        
        assertEquals(null, storage.getDecidedWaitingExecution(0));
        assertEquals(null, storage.getDecidedWaitingExecution(1));
        assertEquals(new PersistentConsensusInstance(2), storage.getDecidedWaitingExecution(2));
        assertEquals(null, storage.getDecidedWaitingExecution(3));
        
        storage.releaseDecidedWaitingExecution(2);
        assertEquals(0, storage.decidedWaitingExecutionCount());
        
        storage.addDecidedWaitingExecution(3, null);
        storage.addDecidedWaitingExecution(4, null);
        storage.addDecidedWaitingExecution(5, null);
        
        assertEquals(null, storage.getDecidedWaitingExecution(2));
        assertEquals(new PersistentConsensusInstance(3), storage.getDecidedWaitingExecution(3));
        assertEquals(new PersistentConsensusInstance(4), storage.getDecidedWaitingExecution(4));
        assertEquals(new PersistentConsensusInstance(5), storage.getDecidedWaitingExecution(5));
        assertEquals(null, storage.getDecidedWaitingExecution(6));
        
        assertEquals(3, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecutionUpTo(2);
        assertEquals(3, storage.decidedWaitingExecutionCount());
        
        assertEquals(3, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecutionUpTo(4);
        assertEquals(2, storage.decidedWaitingExecutionCount());
        
        assertEquals(null, storage.getDecidedWaitingExecution(2));
        assertEquals(null, storage.getDecidedWaitingExecution(3));
        assertEquals(new PersistentConsensusInstance(4), storage.getDecidedWaitingExecution(4));
        assertEquals(new PersistentConsensusInstance(5), storage.getDecidedWaitingExecution(5));
        assertEquals(null, storage.getDecidedWaitingExecution(6));
        
        assertEquals(2, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecutionUpTo(6);
        assertEquals(0, storage.decidedWaitingExecutionCount());        
        
        assertEquals(null, storage.getDecidedWaitingExecution(2));
        assertEquals(null, storage.getDecidedWaitingExecution(3));
        assertEquals(null, storage.getDecidedWaitingExecution(4));
        assertEquals(null, storage.getDecidedWaitingExecution(5));
        assertEquals(null, storage.getDecidedWaitingExecution(6));
        
        storage.releaseDecidedWaitingExecutionUpTo(10);
        
        assertEquals(null, storage.getDecidedWaitingExecution(8));
        assertEquals(null, storage.getDecidedWaitingExecution(9));
        assertEquals(null, storage.getDecidedWaitingExecution(10));
        assertEquals(null, storage.getDecidedWaitingExecution(11));
        assertEquals(null, storage.getDecidedWaitingExecution(12));
        
        storage.addDecidedWaitingExecution(12, null);
        storage.addDecidedWaitingExecution(14, null);
        storage.addDecidedWaitingExecution(16, null);
        
        storage.releaseDecidedWaitingExecutionUpTo(15);
        
        assertEquals(1, storage.decidedWaitingExecutionCount());        
        
        assertEquals(null, storage.getDecidedWaitingExecution(12));
        assertEquals(null, storage.getDecidedWaitingExecution(13));
        assertEquals(null, storage.getDecidedWaitingExecution(14));
        assertEquals(null, storage.getDecidedWaitingExecution(15));
        assertEquals(new PersistentConsensusInstance(16), storage.getDecidedWaitingExecution(16));
        assertEquals(null, storage.getDecidedWaitingExecution(17));
        
        for(int i = 1000 ; i < 2000; ++i) {
            storage.addDecidedWaitingExecution(i, null);
        }
        
        storage.releaseDecidedWaitingExecutionUpTo(1500);
    }
}

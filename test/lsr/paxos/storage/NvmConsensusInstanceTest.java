package lsr.paxos.storage;

import static lsr.common.ProcessDescriptor.processDescriptor;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import lsr.paxos.NvmTestTemplate;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NvmConsensusInstanceTest extends NvmTestTemplate {
    
    PersistentStorage storage = new PersistentStorage();
    PersistentLog log = storage.getLog();
    PersistentConsensusInstance ci0 = log.getInstance(0);
    PersistentConsensusInstance ci1 = log.getInstance(1);
    PersistentConsensusInstance ci2 = log.getInstance(2);
    PersistentConsensusInstance ci3 = log.getInstance(3);
    PersistentConsensusInstance ci4 = log.getInstance(4);
    
    final byte[] value02 = {'a', 'b', 'c'};
    final byte[] value08 = {'A', 'B', 'C'};
    final byte[] value09 = {'A', 'B', 'D'};
    final byte[] value12 = {'d', 'e'};
    final byte[] value18 = {'f'};
    final byte[] value21 = {'g', 'h', 'i', 'j'};
    final byte[] value33 = {'k', 'l', 'm', 'n', 'o'};
    
    
    @Test
    public void a_updateFromPropose() {
       assertEquals(-1, ci0.getLastSeenView());
       assertEquals(-1, ci0.getLastVotedView());
       assertEquals(null, ci0.getValue());
       assertEquals(LogEntryState.UNKNOWN, ci0.getState());
        
       boolean maj = ci0.updateStateFromPropose(2, 2, value02);
       
       assertEquals(2, ci0.getLastSeenView());
       assertEquals(2, ci0.getLastVotedView());
       assertArrayEquals(value02, ci0.getValue());
       assertEquals(LogEntryState.KNOWN, ci0.getState());
       assertEquals(false, maj);
       
       assertEquals(-1, ci1.getLastSeenView());
       assertEquals(-1, ci1.getLastVotedView());
       assertEquals(null, ci1.getValue());
       assertEquals(LogEntryState.UNKNOWN, ci1.getState());
       
       
       maj = ci1.updateStateFromPropose(2, 2, value12);
       
       assertEquals(2, ci1.getLastSeenView());
       assertEquals(2, ci1.getLastVotedView());
       assertArrayEquals(value12, ci1.getValue());
       assertEquals(LogEntryState.KNOWN, ci1.getState());
       assertEquals(false, maj);
       
       maj = ci1.updateStateFromPropose(3, 8, value18);
       
       assertEquals(8, ci1.getLastSeenView());
       assertEquals(8, ci1.getLastVotedView());
       assertArrayEquals(value18, ci1.getValue());
       assertEquals(LogEntryState.KNOWN, ci1.getState());
       assertEquals(false, maj);
    }
    
    @Test
    public void b_updateFromAccept() {
        // 0, 1 (me), 3 (leader) == majority
        boolean maj = ci1.updateStateFromAccept(8, 0);
        assertEquals(true, maj);
        
        maj = ci1.updateStateFromAccept(8, 2);
        assertEquals(true, maj);
        
        maj = ci1.updateStateFromAccept(8, 4);
        assertEquals(true, maj);
        
        assertEquals(8, ci1.getLastSeenView());
        assertEquals(8, ci1.getLastVotedView());
        assertArrayEquals(value18, ci1.getValue());
        assertEquals(LogEntryState.KNOWN, ci1.getState());
        
        
        assert(processDescriptor.localId==1);
        maj = ci2.updateStateFromPropose(processDescriptor.localId, 1, value21);
        assertEquals(false, maj);
        
        assertEquals(1, ci2.getLastSeenView());
        assertEquals(1, ci2.getLastVotedView());
        assertArrayEquals(value21, ci2.getValue());
        assertEquals(LogEntryState.KNOWN, ci2.getState());
        
        maj = ci2.updateStateFromAccept(1, 3);
        assertEquals(false, maj);
        
        maj = ci2.updateStateFromAccept(1, 3);
        assertEquals(false, maj);
        
        maj = ci2.updateStateFromAccept(1, 0);
        assertEquals(true, maj);
        
        assertEquals(1, ci2.getLastSeenView());
        assertEquals(1, ci2.getLastVotedView());
        assertArrayEquals(value21, ci2.getValue());
        assertEquals(LogEntryState.KNOWN, ci2.getState());
    }
    
    @Test
    public void c_resetInstance() {
        assertEquals(2, ci0.getLastSeenView());
        assertEquals(2, ci0.getLastVotedView());
        assertArrayEquals(value02, ci0.getValue());
        assertEquals(LogEntryState.KNOWN, ci0.getState());
        
        // 1/5
        boolean maj = ci0.updateStateFromAccept(8, 0);
        assertEquals(false, maj);
        
        assertEquals(8, ci0.getLastSeenView());
        assertEquals(2, ci0.getLastVotedView());
        assertArrayEquals(value02, ci0.getValue());
        assertEquals(LogEntryState.RESET, ci0.getState());
        
        // 2/5
        maj = ci0.updateStateFromAccept(8, 2);
        assertEquals(false, maj);
        assertEquals(false, ci0.isMajority());
        
        // 3/5
        maj = ci0.updateStateFromAccept(8, 4);
        assertEquals(false, maj);
        assertEquals(true, ci0.isMajority());
        
        assertEquals(8, ci0.getLastSeenView());
        assertEquals(2, ci0.getLastVotedView());
        assertArrayEquals(value02, ci0.getValue());
        assertEquals(LogEntryState.RESET, ci0.getState());
        
        
        maj = ci0.updateStateFromPropose(3, 8, value08);
        assertEquals(true, maj);
        
        assertEquals(8, ci0.getLastSeenView());
        assertEquals(8, ci0.getLastVotedView());
        assertArrayEquals(value08, ci0.getValue());
        assertEquals(LogEntryState.KNOWN, ci0.getState());
    }
    
    @Test
    public void d_updateFromDecided() {
        assertEquals(8, ci0.getLastSeenView());
        assertEquals(8, ci0.getLastVotedView());
        assertArrayEquals(value08, ci0.getValue());
        assertEquals(LogEntryState.KNOWN, ci0.getState());
        
        ci0.updateStateFromAccept(9,  4);
        
        assertEquals(LogEntryState.RESET, ci0.getState());
        
        ci0.updateStateFromDecision(9, value09);
        
        assertEquals(9, ci0.getLastSeenView());
        assertEquals(9, ci0.getLastVotedView());
        assertArrayEquals(value09, ci0.getValue());
        assertEquals(LogEntryState.KNOWN, ci0.getState());
        
        ci0.setDecided();
        
        assertEquals(LogEntryState.DECIDED, ci0.getState());
    }
    
    @Test
    public void e_writingToBB() throws IOException {
        
        // DECIDED
        
        assertEquals(9, ci0.getLastSeenView());
        assertEquals(9, ci0.getLastVotedView());
        assertArrayEquals(value09, ci0.getValue());
        assertEquals(LogEntryState.DECIDED, ci0.getState());
        
        // direct
        ByteBuffer dbb = ByteBuffer.allocateDirect(ci0.byteSize());
        ci0.writeAsLastVoted(dbb);
        byte[] darray = new byte[ci0.byteSize()];
        dbb.get(darray);
        
        // non-direct
        byte array[] = new byte[ci0.byteSize()];
        ci0.writeAsLastVoted(ByteBuffer.wrap(array));
        
        assertArrayEquals(array, darray);
        
        InMemoryConsensusInstance imci = new InMemoryConsensusInstance(new DataInputStream(new ByteArrayInputStream(array)));
        
        assertEquals(9, ci0.getLastSeenView());
        assertEquals(9, ci0.getLastVotedView());
        assertArrayEquals(value09, ci0.getValue());
        assertEquals(LogEntryState.DECIDED, ci0.getState());
        
        assertEquals(9, imci.getLastSeenView());
        assertEquals(9, imci.getLastVotedView());
        assertArrayEquals(value09, imci.getValue());
        assertEquals(LogEntryState.DECIDED, imci.getState());
        
        // KNOWN
        
        assertEquals(1, ci2.getLastSeenView());
        assertEquals(1, ci2.getLastVotedView());
        assertArrayEquals(value21, ci2.getValue());
        assertEquals(LogEntryState.KNOWN, ci2.getState());
        
        array = new byte[ci2.byteSize()];
        ci2.writeAsLastVoted(ByteBuffer.wrap(array));
        imci = new InMemoryConsensusInstance(new DataInputStream(new ByteArrayInputStream(array)));

        assertEquals(1, imci.getLastSeenView());
        assertEquals(1, imci.getLastVotedView());
        assertArrayEquals(value21, imci.getValue());
        assertEquals(LogEntryState.KNOWN, imci.getState());
        
        // RESET
        
        ci3.updateStateFromPropose(3, 3, value33);
        ci3.updateStateFromAccept(4, 4);
        
        assertEquals(4, ci3.getLastSeenView());
        assertEquals(3, ci3.getLastVotedView());
        assertArrayEquals(value33, ci3.getValue());
        assertEquals(LogEntryState.RESET, ci3.getState());
        
        array = new byte[ci3.byteSize()];
        ci3.writeAsLastVoted(ByteBuffer.wrap(array));
        imci = new InMemoryConsensusInstance(new DataInputStream(new ByteArrayInputStream(array)));

        assertEquals(3, imci.getLastSeenView());
        assertEquals(3, imci.getLastVotedView());
        assertArrayEquals(value33, imci.getValue());
        assertEquals(LogEntryState.KNOWN, imci.getState());
        
        
        // UNKNOWN
        
        assertEquals(-1, ci4.getLastSeenView());
        assertEquals(-1, ci4.getLastVotedView());
        assertEquals(null, ci4.getValue());
        assertEquals(LogEntryState.UNKNOWN, ci4.getState());
         
        array = new byte[ci4.byteSize()];
        ci4.writeAsLastVoted(ByteBuffer.wrap(array));
        imci = new InMemoryConsensusInstance(new DataInputStream(new ByteArrayInputStream(array)));

        assertEquals(-1, imci.getLastSeenView());
        assertEquals(-1, imci.getLastVotedView());
        assertEquals(null, imci.getValue());
        assertEquals(LogEntryState.UNKNOWN, imci.getState());
    }

}

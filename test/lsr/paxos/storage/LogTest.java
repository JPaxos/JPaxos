package lsr.paxos.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

import org.junit.Before;
import org.junit.Test;

public class LogTest {
    private Log log;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        log = new Log();
    }

    @Test
    public void shouldBeEmptyAfterCreation() {
        assertEquals(0, log.getInstanceMap().size());
        assertEquals(0, log.getNextId());
        assertEquals(0, log.getLowestAvailableId());
    }

    @Test
    public void shouldCreateEmptyInstancesAfterGetInstanceMethod() {
        ConsensusInstance instance = log.getInstance(2);

        assertEquals(3, log.getNextId());
        assertEquals(3, log.getInstanceMap().size());
        assertEquals(2, instance.getId());
        assertEquals(-1, instance.getView());
        assertEquals(null, instance.getValue());
        assertEquals(LogEntryState.UNKNOWN, instance.getState());
    }

    @Test
    public void shouldAppendNewInstance() {
        log.getInstance(2);
        ConsensusInstance instance = log.append(11, new byte[] {1, 2, 3});

        assertEquals(4, log.getNextId());
        assertEquals(4, log.getInstanceMap().size());
        assertEquals(3, instance.getId());
        assertEquals(11, instance.getView());
        assertArrayEquals(new byte[] {1, 2, 3}, instance.getValue());
        assertEquals(LogEntryState.KNOWN, instance.getState());
    }

    @Test
    public void shouldNotifyListenersAboutLogSizeChange() {
        LogListener listener = mock(LogListener.class);
        log.addLogListener(listener);

        log.getInstance(3);
        verify(listener).logSizeChanged(4);

        log.append(3, new byte[] {1, 2, 3});
        verify(listener).logSizeChanged(5);

        log.removeLogListener(listener);
        log.append(4, new byte[] {1, 2, 3});
        verify(listener, never()).logSizeChanged(6);
    }

    @Test
    public void shouldReturnSizeOfTheLog() {
        log.getInstance(3);
        assertEquals(4, log.size());
    }

    @Test
    public void shouldTruncateInstancesBelow() {
        // create instances [0, 10]
        log.getInstance(10);

        // remove instances [0, 4] from log
        log.truncateBelow(5);

        for (int i = 0; i < 4; i++) {
            assertEquals(null, log.getInstance(i));
        }

        assertEquals(6, log.size());
        assertEquals(5, log.getLowestAvailableId());
    }

    @Test
    public void shouldTruncateInstancesBelowWhenLogIsEmpty() {
        // remove instances [0, 4] from log
        log.truncateBelow(5);

        for (int i = 0; i < 4; i++) {
            assertEquals(null, log.getInstance(i));
        }

        assertEquals(0, log.size());
        assertEquals(5, log.getLowestAvailableId());
        assertEquals(5, log.getNextId());
    }

    @Test
    public void shouldTruncateAllInstances() {
        // create instances [0, 10]
        log.getInstance(10);

        // remove instances [0, 11] from log
        log.truncateBelow(12);

        for (int i = 0; i < 11; i++) {
            assertEquals(null, log.getInstance(i));
        }

        assertEquals(0, log.size());
        assertEquals(12, log.getLowestAvailableId());
        assertEquals(12, log.getNextId());
    }

    @Test
    public void shouldClearUndecidedInstances() {
        // create instances [0, 9]
        for (int i = 0; i < 10; i++) {
            log.append(5, new byte[] {1, 2, 3});
        }

        log.getInstance(1).setDecided();
        log.getInstance(3).setDecided();
        log.getInstance(5).setDecided();
        log.getInstance(6).setDecided();
        log.getInstance(7).setDecided();

        // remove instances [0, 2, 4, 8]
        log.clearUndecidedBelow(9);

        assertEquals(6, log.size());
        // TODO TZ - instance 1 is lowest available
        assertEquals(9, log.getLowestAvailableId());
        assertEquals(10, log.getNextId());
        assertNull(log.getInstance(0));
        assertNull(log.getInstance(2));
        assertNull(log.getInstance(4));
        assertNull(log.getInstance(8));
        assertNotNull(log.getInstance(1));
    }

    @Test
    public void shouldClearUndecidedInstancesWhenLogIsEmpty() {
        log.clearUndecidedBelow(5);

        assertEquals(0, log.size());
        // TODO TZ - should it update lowest available and next id?
        assertEquals(0, log.getLowestAvailableId());
        assertEquals(0, log.getNextId());
    }

    @Test
    public void shouldCalculateSizeBetweenTwoInstances() {
        for (int i = 0; i < 10; i++) {
            log.append(4, new byte[] {1, 2, 3, 4, 5});
        }

        long size = log.byteSizeBetween(3, 7);

        long expectedSize = log.getInstance(3).byteSize() +
                            log.getInstance(4).byteSize() +
                            log.getInstance(5).byteSize() +
                            log.getInstance(6).byteSize();

        assertEquals(expectedSize, size);
    }

    @Test
    public void shouldCalculateSizeBetweenTwoInstanceIfSomeAreRemoved() {
        // create instances [0, 9]
        for (int i = 0; i < 10; i++) {
            log.append(5, new byte[] {1, 2, 3});
        }

        log.getInstance(1).setDecided();
        log.getInstance(3).setDecided();
        log.getInstance(5).setDecided();
        log.getInstance(6).setDecided();
        log.getInstance(7).setDecided();

        // remove instances [0, 2, 4, 8]
        log.clearUndecidedBelow(9);

        long size = log.byteSizeBetween(3, 7);

        long expectedSize = log.getInstance(3).byteSize() +
                            log.getInstance(5).byteSize() +
                            log.getInstance(6).byteSize();

        assertEquals(expectedSize, size);
    }
}

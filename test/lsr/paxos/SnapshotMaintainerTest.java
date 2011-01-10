package lsr.paxos;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.recovery.MockDispatcher;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;

public class SnapshotMaintainerTest {
    private Storage storage;
    private Log log;
    private MockDispatcher dispatcher;
    private SnapshotProvider snapshotProvider;
    private SnapshotMaintainer snapshotMaintainer;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        storage = mock(Storage.class);
        log = mock(Log.class);
        when(storage.getLog()).thenReturn(log);
        dispatcher = new MockDispatcher();
        snapshotProvider = mock(SnapshotProvider.class);
        snapshotMaintainer = new SnapshotMaintainer(storage, dispatcher, snapshotProvider);
        dispatcher.forceBeingInDispatcher();
    }

    @Test
    public void shouldHandleFirstSnapshot() {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(10);
        snapshot.setValue(new byte[] {1, 2, 3, 4});

        snapshotMaintainer.onSnapshotMade(snapshot);
        dispatcher.execute();

        verify(storage).setLastSnapshot(snapshot);
        verify(log).truncateBelow(0);
    }

    @Test
    public void shouldHandleNextSnapshot() {
        Snapshot lastSnapshot = new Snapshot();
        lastSnapshot.setNextInstanceId(5);
        when(storage.getLastSnapshot()).thenReturn(lastSnapshot);

        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(10);
        snapshot.setValue(new byte[] {1, 2, 3, 4});

        snapshotMaintainer.onSnapshotMade(snapshot);
        dispatcher.execute();

        verify(storage).setLastSnapshot(snapshot);
        verify(log).truncateBelow(5);
    }

    @Test
    public void shouldIgnoreOlderSnapshots() {
        Snapshot lastSnapshot = new Snapshot();
        lastSnapshot.setNextInstanceId(10);
        when(storage.getLastSnapshot()).thenReturn(lastSnapshot);

        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(5);
        snapshot.setValue(new byte[] {1, 2, 3, 4});

        snapshotMaintainer.onSnapshotMade(snapshot);
        dispatcher.execute();

        verify(storage).getLastSnapshot();
        verifyNoMoreInteractions(storage);
        verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldAskForSnapshot() {
        when(log.getNextId()).thenReturn(50);
        when(log.byteSizeBetween(anyInt(), anyInt())).thenReturn((long) 20000);
        snapshotMaintainer.logSizeChanged(1);

        verify(snapshotProvider).askForSnapshot();
    }

    @Test
    public void shouldNotAskForSnapshotIfLogIsTooSmall() {
        when(log.getNextId()).thenReturn(10);
        when(log.byteSizeBetween(anyInt(), anyInt())).thenReturn((long) 20000);
        snapshotMaintainer.logSizeChanged(1);

        verifyZeroInteractions(snapshotProvider);
    }

    @Test
    public void shouldNotAskForSnapshotIfLogByteSizeIsTooSmall() {
        when(log.getNextId()).thenReturn(50);
        when(log.byteSizeBetween(anyInt(), anyInt())).thenReturn((long) 10000);
        snapshotMaintainer.logSizeChanged(1);

        verifyZeroInteractions(snapshotProvider);
    }

    @Test
    public void shouldForceSnapshot() {
        when(log.getNextId()).thenReturn(50);
        when(log.byteSizeBetween(anyInt(), anyInt())).thenReturn((long) 20000);
        snapshotMaintainer.logSizeChanged(1);

        when(log.getNextId()).thenReturn(100);
        snapshotMaintainer.logSizeChanged(1);

        verify(snapshotProvider).forceSnapshot();
    }

    @Test
    public void shouldForceSnapshotOnlyOnce() {
        when(log.getNextId()).thenReturn(50);
        when(log.byteSizeBetween(anyInt(), anyInt())).thenReturn((long) 20000);
        snapshotMaintainer.logSizeChanged(1);

        when(log.getNextId()).thenReturn(100);
        snapshotMaintainer.logSizeChanged(1);

        verify(snapshotProvider).askForSnapshot();
        verify(snapshotProvider).forceSnapshot();

        when(log.getNextId()).thenReturn(150);
        snapshotMaintainer.logSizeChanged(1);

        verifyNoMoreInteractions(snapshotProvider);
    }

    @Test
    public void shouldWaitBetweenAskAndForceSnapshot() {
        when(log.getNextId()).thenReturn(50);
        when(log.byteSizeBetween(anyInt(), anyInt())).thenReturn((long) 20000);
        snapshotMaintainer.logSizeChanged(10);

        verify(snapshotProvider).askForSnapshot();

        when(log.getNextId()).thenReturn(51);
        snapshotMaintainer.logSizeChanged(1);

        verifyNoMoreInteractions(snapshotProvider);
    }

    @Test
    public void shouldStartAskingAgainAfterReceivingNewSnapshot() {
        when(log.getNextId()).thenReturn(50);
        when(log.byteSizeBetween(anyInt(), anyInt())).thenReturn((long) 20000);
        snapshotMaintainer.logSizeChanged(1);
        when(log.getNextId()).thenReturn(100);
        snapshotMaintainer.logSizeChanged(1);
        reset(snapshotProvider);

        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(10);
        snapshot.setValue(new byte[] {1, 2, 3, 4});

        snapshotMaintainer.onSnapshotMade(snapshot);
        dispatcher.execute();

        when(log.getNextId()).thenReturn(150);
        snapshotMaintainer.logSizeChanged(1);
        verify(snapshotProvider).askForSnapshot();
    }
}

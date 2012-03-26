package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import lsr.paxos.Snapshot;

import org.junit.Before;
import org.junit.Test;

public class SynchronousStorageTest {

    private DiscWriter writer;
    private Storage storage;

    @Before
    public void setUp() throws IOException {
        writer = mock(DiscWriter.class);
    }

    @Test
    public void shouldWriteNewView() throws IOException {
        storage = new SynchronousStorage(writer);
        storage.setView(4);
        verify(writer).changeViewNumber(4);
    }

    @Test
    public void shouldLoadView() throws IOException {
        when(writer.loadViewNumber()).thenReturn(5);
        storage = new SynchronousStorage(writer);
        assertEquals(5, storage.getView());
    }

    @Test
    public void shouldSaveLastSnapshot() throws IOException {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(5);

        storage = new SynchronousStorage(writer);
        storage.setLastSnapshot(snapshot);

        verify(writer).newSnapshot(snapshot);
    }

    @Test
    public void shouldLoadSnapshot() throws IOException {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(5);

        when(writer.getSnapshot()).thenReturn(snapshot);

        storage = new SynchronousStorage(writer);

        assertEquals(snapshot, storage.getLastSnapshot());
    }
}

package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

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
}

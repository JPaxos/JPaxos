package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class SynchronousViewStorageTest {
    @Test
    public void shouldReadViewAfterCreation() {
        SingleNumberWriter writer = mock(SingleNumberWriter.class);
        when(writer.readNumber()).thenReturn((long) 5);

        SynchronousViewStorage storage = new SynchronousViewStorage(writer);

        assertEquals(5, storage.getView());
    }

    @Test
    public void shouldWriteViewOnViewChange() {
        SingleNumberWriter writer = mock(SingleNumberWriter.class);
        when(writer.readNumber()).thenReturn((long) 5);
        SynchronousViewStorage storage = new SynchronousViewStorage(writer);

        storage.setView(10);

        verify(writer).writeNumber(10);
        assertEquals(10, storage.getView());
    }
}

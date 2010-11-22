package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Test;

public class SynchronousStableStorageTest {
	@Test
	public void testSetView() throws IOException {
		DiscWriter writer = mock(DiscWriter.class);
		StableStorage storage = new SynchronousStableStorage(writer);
		storage.setView(4);
		verify(writer).changeViewNumber(4);
	}

	@Test
	public void loadsView() throws IOException {
		DiscWriter writer = mock(DiscWriter.class);
		when(writer.loadViewNumber()).thenReturn(5);
		StableStorage storage = new SynchronousStableStorage(writer);
		assertEquals(storage.getView(), 5);
	}
}

package lsr.paxos.storage;

import java.io.IOException;

import lsr.paxos.storage.DiscWriter;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.SynchronousStableStorage;

import org.testng.annotations.Test;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test
public class SynchronousStableStorageTest {

	public void testSetView() throws IOException {
		DiscWriter writer = mock(DiscWriter.class);
		StableStorage storage = new SynchronousStableStorage(writer);
		storage.setView(4);
		verify(writer).changeViewNumber(4);
	}

	public void loadsView() throws IOException {
		DiscWriter writer = mock(DiscWriter.class);
		when(writer.loadViewNumber()).thenReturn(5);
		StableStorage storage = new SynchronousStableStorage(writer);
		assertEquals(storage.getView(), 5);
	}
}

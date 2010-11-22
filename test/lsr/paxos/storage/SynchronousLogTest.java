package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import lsr.paxos.storage.ConsensusInstance.LogEntryState;

import org.junit.Test;

public class SynchronousLogTest {

	@Test
	public void testAppend() throws IOException {
		DiscWriter writer = mock(DiscWriter.class);
		SynchronousLog log = new SynchronousLog(writer);

		int view = 1;
		byte[] value = new byte[] { 1, 2, 3 };
		ConsensusInstance instance = log.append(view, value);
		verify(writer).changeInstanceValue(0, view, value);
		assertTrue(instance instanceof SynchronousConsensusInstace);
	}

	@Test
	public void testCreatesSynchronousConsensusInstance() throws IOException {
		DiscWriter writer = mock(DiscWriter.class);
		SynchronousLog log = new SynchronousLog(writer);

		ConsensusInstance instance = log.getInstance(0);
		assertTrue(instance instanceof SynchronousConsensusInstace);
	}

	@Test
	public void testInitialInstances() throws IOException {
		ConsensusInstance instance1 = new ConsensusInstance(1, LogEntryState.KNOWN, 1, new byte[] { 1 });
		ConsensusInstance instance2 = new ConsensusInstance(2, LogEntryState.KNOWN, 1, new byte[] { 1 });
		Collection<ConsensusInstance> instances = new ArrayList<ConsensusInstance>();
		instances.add(instance1);
		instances.add(instance2);

		DiscWriter writer = mock(DiscWriter.class);
		when(writer.load()).thenReturn(instances);
		SynchronousLog log = new SynchronousLog(writer);

		areEquals(log.getInstance(1), instance1);
		areEquals(log.getInstance(2), instance2);
		assertTrue(log.getInstance(1) instanceof SynchronousConsensusInstace);
		assertTrue(log.getInstance(2) instanceof SynchronousConsensusInstace);
	}

	@Test
	public void testNotSortedInitialInstances() throws IOException {
		ConsensusInstance instance1 = new ConsensusInstance(1, LogEntryState.KNOWN, 1, new byte[] { 1 });
		ConsensusInstance instance2 = new ConsensusInstance(2, LogEntryState.KNOWN, 1, new byte[] { 1 });
		Collection<ConsensusInstance> instances = new ArrayList<ConsensusInstance>();
		instances.add(instance2);
		instances.add(instance1);

		DiscWriter writer = mock(DiscWriter.class);
		when(writer.load()).thenReturn(instances);
		SynchronousLog log = new SynchronousLog(writer);

		areEquals(log.getInstance(1), instance1);
		areEquals(log.getInstance(2), instance2);
		assertTrue(log.getInstance(1) instanceof SynchronousConsensusInstace);
		assertTrue(log.getInstance(2) instanceof SynchronousConsensusInstace);
	}

	private void areEquals(ConsensusInstance actual, ConsensusInstance expected) {
		assertEquals(actual.getId(), expected.getId());
		assertEquals(actual.getState(), expected.getState());
		assertEquals(actual.getView(), expected.getView());
		assertEquals(actual.getValue(), expected.getValue());
	}
}

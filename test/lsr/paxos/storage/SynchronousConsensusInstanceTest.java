package lsr.paxos.storage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

public class SynchronousConsensusInstanceTest {
	private DiscWriter writer;
	private SynchronousConsensusInstace instance;
	private int view;
	private byte[] values;

	@Before
	public void setUp() {
		writer = mock(DiscWriter.class);
		instance = new SynchronousConsensusInstace(2, writer);

		view = 1;
		values = new byte[] { 1, 2, 3 };
	}

	@Test
	public void testChangeValueOnEmptyInstance() {
		instance.setValue(view, values);
		verify(writer).changeInstanceValue(2, view, values);
	}

	@Test
	public void testChangeViewOnEmptyInstance() {
		instance.setView(view);
		verify(writer).changeInstanceView(2, view);
	}

	@Test
	public void testSettingTheSameViewTwiceNotWritesToDisc() {
		instance.setView(view);
		instance.setView(view);
		verify(writer, times(1)).changeInstanceView(2, view);
	}

	@Test
	public void testSettingTheSameValueTwiceWritesJustView() {
		instance.setValue(view, values);
		instance.setValue(view + 1, values);
		verify(writer, times(1)).changeInstanceValue(2, view, values);
		verify(writer, times(1)).changeInstanceView(2, view + 1);
	}
}

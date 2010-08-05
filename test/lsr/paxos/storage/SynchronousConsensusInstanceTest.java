package lsr.paxos.storage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import lsr.paxos.storage.DiscWriter;
import lsr.paxos.storage.SynchronousConsensusInstace;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "unit" })
public class SynchronousConsensusInstanceTest {
	private DiscWriter _writer;
	private SynchronousConsensusInstace _instance;
	private int _view;
	private byte[] _values;

	@BeforeMethod
	public void setUp() {
		_writer = mock(DiscWriter.class);
		_instance = new SynchronousConsensusInstace(2, _writer);

		_view = 1;
		_values = new byte[] { 1, 2, 3 };
	}

	public void testChangeValueOnEmptyInstance() {
		_instance.setValue(_view, _values);
		verify(_writer).changeInstanceValue(2, _view, _values);
	}

	public void testChangeViewOnEmptyInstance() {
		_instance.setView(_view);
		verify(_writer).changeInstanceView(2, _view);
	}

	public void testSettingTheSameViewTwiceNotWritesToDisc() {
		_instance.setView(_view);
		_instance.setView(_view);
		verify(_writer, times(1)).changeInstanceView(2, _view);
	}

	public void testSettingTheSameValueTwiceWritesJustView() {
		_instance.setValue(_view, _values);
		_instance.setValue(_view + 1, _values);
		verify(_writer, times(1)).changeInstanceValue(2, _view, _values);
		verify(_writer, times(1)).changeInstanceView(2, _view + 1);
	}
}

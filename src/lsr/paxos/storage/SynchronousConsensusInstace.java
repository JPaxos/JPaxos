package lsr.paxos.storage;

import java.util.Arrays;

public class SynchronousConsensusInstace extends ConsensusInstance {
	private final DiscWriter _writer;

	public SynchronousConsensusInstace(Integer nextId, LogEntryState known, int view, byte[] value, DiscWriter writer) {
		super(nextId, known, view, value);
		_writer = writer;
	}

	public SynchronousConsensusInstace(Integer nextId, DiscWriter writer) {
		super(nextId);
		_writer = writer;
	}

	public SynchronousConsensusInstace(ConsensusInstance instance, DiscWriter writer) {
		super(instance.getId(), instance.getState(), instance.getView(), instance.getValue());
		_writer = writer;
	}

	@Override
	public void setValue(int view, byte[] value) {
		if (view < _view)
			throw new RuntimeException("Tried to set old value!");
		if (view == _view) {
			assert _value == null || Arrays.equals(value, _value);

			if (_value == null && value != null) {
				_writer.changeInstanceValue(_id, view, value);
				_value = value;
			}

		} else // view > _view
		{
			if (Arrays.equals(_value, value)) {
				setView(view);
				_view = view;
			} else {
				_writer.changeInstanceValue(_id, view, value);
				_value = value;
				_view = view;
			}
		}

		if (_state != LogEntryState.DECIDED) {
			if (_value != null)
				_state = LogEntryState.KNOWN;
			else
				_state = LogEntryState.UNKNOWN;
		}

	}

	public void setView(int view) {
		assert _view <= view : "Cannot set smaller view.";
		if (_view != view) {
			_writer.changeInstanceView(_id, view);
			_view = view;
		}
	}

	@Override
	public void setDecided() {
		super.setDecided();
		_writer.decideInstance(_id);
	}

	private static final long serialVersionUID = 1L;
}

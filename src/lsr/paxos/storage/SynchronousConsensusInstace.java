package lsr.paxos.storage;

import java.util.Arrays;
import java.util.BitSet;

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
		_executeMarker = instance.getExecuteMarker();
		_executeSeqNo = instance.getStartingExecuteSeqNo();
	}

	@Override
	public void setValue(int view, byte[] value) {
		if (view < _view)
			return;

		if (_state == LogEntryState.UNKNOWN)
			_state = LogEntryState.KNOWN;

		if (_value == null || !Arrays.equals(value, _value)) {
			// save view and value
			assert _state != LogEntryState.DECIDED : "Cannot change value in decided instance.";
			assert _view != view : "Different value for the same view";
			_writer.changeInstanceValue(_id, view, value);
			_view = view;
			_value = value;
		} else {
			// save just view
			setView(view);
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

	@Override
	public void setSeqNoAndMarkers(int executeSeqNo, BitSet executeMarker) {
		if (_executeSeqNo != executeSeqNo || (!executeMarker.equals(_executeMarker))) {
			_writer.changeInstanceSeqNoAndMarkers(_id, executeSeqNo, executeMarker);
			_executeSeqNo = executeSeqNo;
			_executeMarker = (BitSet) executeMarker.clone();
		}
	}

	private static final long serialVersionUID = 1L;
}

package lsr.paxos.storage;

import java.io.IOException;
import java.util.Collection;

import lsr.paxos.Log;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class SynchronousLog extends Log {
	private final DiscWriter _writer;

	public SynchronousLog(DiscWriter writer) throws IOException {
		_writer = writer;
		Collection<ConsensusInstance> instances = writer.load();

		for (ConsensusInstance instance : instances) {
			while (_nextId < instance.getId()) {
				_instances.put(_nextId, createInstance());
				_nextId++;
			}
			_nextId++;
			assert instance.getState() != LogEntryState.UNKNOWN;
			ConsensusInstance i = new SynchronousConsensusInstace(instance, _writer);
			_instances.put(instance.getId(), i);
		}
	}

	@Override
	protected ConsensusInstance createInstance() {
		return new SynchronousConsensusInstace(_nextId, _writer);
	}

	@Override
	protected ConsensusInstance createInstance(int view, byte[] value) {
		_writer.changeInstanceValue(_nextId, view, value);
		return new SynchronousConsensusInstace(_nextId, LogEntryState.KNOWN, view, value, _writer);
	}

	// TODO TZ truncateBelow
	// TODO TZ clearUndecidedBelow
}

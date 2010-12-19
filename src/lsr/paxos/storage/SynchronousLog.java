package lsr.paxos.storage;

import java.io.IOException;
import java.util.Collection;

import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class SynchronousLog extends Log {
    private final DiscWriter writer;

    public SynchronousLog(DiscWriter writer) throws IOException {
        this.writer = writer;
        Collection<ConsensusInstance> instances = writer.load();

        for (ConsensusInstance instance : instances) {
            while (nextId < instance.getId()) {
                this.instances.put(nextId, createInstance());
                nextId++;
            }
            nextId++;

            ConsensusInstance i = new SynchronousConsensusInstace(instance, this.writer);
            this.instances.put(instance.getId(), i);
        }
    }

    protected ConsensusInstance createInstance() {
        return new SynchronousConsensusInstace(nextId, writer);
    }

    protected ConsensusInstance createInstance(int view, byte[] value) {
        writer.changeInstanceValue(nextId, view, value);
        return new SynchronousConsensusInstace(nextId, LogEntryState.KNOWN, view, value, writer);
    }

    // TODO TZ truncateBelow
    // TODO TZ clearUndecidedBelow
}

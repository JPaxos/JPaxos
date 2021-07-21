package lsr.paxos.storage;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class SynchronousLog extends InMemoryLog {
    private final DiscWriter writer;

    public SynchronousLog(DiscWriter writer) throws IOException {
        this.writer = writer;
        Collection<ConsensusInstance> instances = writer.load();

        for (ConsensusInstance instance : instances) {
            logger.debug("SyncLog loaded: {}", instance.toString());
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

    /*
     * TODO: TZ suggested implementing truncateBelow and clearUndecidedBelow for
     * cutting disk logs. This can be done either as mentioned, or at writing
     * down a snapshot.
     */
    
    private static final Logger logger = LoggerFactory.getLogger(SynchronousLog.class);
}

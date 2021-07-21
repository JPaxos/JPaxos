package lsr.paxos.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronousConsensusInstace extends InMemoryConsensusInstance {
    private final DiscWriter writer;

    public SynchronousConsensusInstace(Integer nextId, LogEntryState known, int view, byte[] value,
                                       DiscWriter writer) {
        super(nextId, known, view, value);
        this.writer = writer;
    }

    public SynchronousConsensusInstace(Integer id, DiscWriter writer) {
        super(id);
        this.writer = writer;
    }

    public SynchronousConsensusInstace(ConsensusInstance instance, DiscWriter writer) {
        super(instance.getId(), instance.getState(), instance.getLastSeenView(),
                instance.getValue());
        this.writer = writer;
    }

    protected void aboutToVote() {
        super.aboutToVote();
        writer.changeInstanceValue(id, lastSeenView, value);
    }

    public void setDecided() {
        super.setDecided();
        writer.decideInstance(id);
    }

    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public int hashCode() {
        return super.hashCode();
    }

    private static final long serialVersionUID = 1L;
    @SuppressWarnings("unused")
    private final static Logger logger = LoggerFactory.getLogger(SynchronousConsensusInstace.class);
}

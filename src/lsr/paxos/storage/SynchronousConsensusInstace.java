package lsr.paxos.storage;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronousConsensusInstace extends ConsensusInstance {
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
        super(instance.getId(), instance.getState(), instance.getView(), instance.getValue());
        this.writer = writer;
    }

    protected void setValue(int view, byte[] value) {
        assert this.view <= view : "Cannot set smaller view.";
        assert value != null : "value cannot be null. View: " + view;

        assert state != LogEntryState.DECIDED
               || Arrays.equals(this.value, value) : view + " " + value + " " + this;
        assert state != LogEntryState.KNOWN
               || view != this.view
               || Arrays.equals(this.value, value) : view + " " + value + " " + this;

        writeViewAndOrValue(view, value);

        if (state != LogEntryState.DECIDED) {
            state = LogEntryState.KNOWN;
        }

        onValueChange();
    }

    private void writeViewAndOrValue(int view, byte[] value) {
        if (view == this.view) {
            if (this.value == null) {
                writer.changeInstanceValue(id, view, value);
                this.value = value;
            }
        } else { // view > this.view
            if (Arrays.equals(this.value, value)) {
                setView(view);
            } else {
                writer.changeInstanceValue(id, view, value);
                this.value = value;
                this.view = view;
            }
        }
    }

    public void setView(int view) {
        assert this.view <= view : "Cannot set smaller view.";
        assert state != LogEntryState.DECIDED || view == this.view;

        if (this.view < view) {
            writer.changeInstanceView(id, view);
            accepts.clear();
            this.view = view;
        }
    }

    public void setDecided() {
        super.setDecided();
        writer.decideInstance(id);
    }

    @Override
    public void updateStateFromDecision(int newView, byte[] newValue) {
        assert newValue != null;
        if (state == LogEntryState.DECIDED) {
            logger.error("Updating a decided instance from a catchup message: {}", this);

            // The value must be the same as the local value. No change.
            assert Arrays.equals(newValue, value) : "Values don't match. New view: " + newView +
                                                    ", local: " + this;
            return;
        }
        writeViewAndOrValue(newView, newValue);

        onValueChange();
    }

    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public int hashCode() {
        return super.hashCode();
    }

    private static final long serialVersionUID = 1L;
    private final static Logger logger = LoggerFactory.getLogger(SynchronousConsensusInstace.class);
}

package lsr.paxos.storage;

import java.util.Arrays;

public class SynchronousConsensusInstace extends ConsensusInstance {
    private final DiscWriter writer;

    public SynchronousConsensusInstace(Integer nextId, LogEntryState known, int view, byte[] value,
                                       DiscWriter writer) {
        super(nextId, known, view, value);
        this.writer = writer;
    }

    public SynchronousConsensusInstace(Integer nextId, DiscWriter writer) {
        super(nextId);
        this.writer = writer;
    }

    public SynchronousConsensusInstace(ConsensusInstance instance, DiscWriter writer) {
        super(instance.getId(), instance.getState(), instance.getView(), instance.getValue());
        this.writer = writer;
    }

    public void setValue(int view, byte[] value) {
        if (view < this.view)
            throw new RuntimeException("Tried to set old value!");
        if (view == this.view) {
            assert this.value == null || Arrays.equals(value, this.value);

            if (this.value == null && value != null) {
                writer.changeInstanceValue(id, view, value);
                this.value = value;
            }
        } else { // view > this.view
            if (Arrays.equals(this.value, value)) {
                setView(view);
                this.view = view;
            } else {
                writer.changeInstanceValue(id, view, value);
                this.value = value;
                this.view = view;
            }
        }

        if (state != LogEntryState.DECIDED) {
            if (this.value != null)
                state = LogEntryState.KNOWN;
            else
                state = LogEntryState.UNKNOWN;
        }
    }

    public void setView(int view) {
        assert this.view <= view : "Cannot set smaller view.";
        if (this.view != view) {
            writer.changeInstanceView(id, view);
            this.view = view;
        }
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
}

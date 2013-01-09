package lsr.paxos.storage;

import java.util.Arrays;

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

    public void setValue(int view, byte[] value) {
        assert this.view <= view : "Cannot set smaller view.";
        assert value != null : "value cannot be null. View: " + view;
        assert state != LogEntryState.DECIDED || view == this.view;

        /*
         * if instance is KNOWN and the view remains unchanged OR if the
         * instance is DECIDED the values must match.
         */
        assert ((state == LogEntryState.KNOWN && view == this.view) || state == LogEntryState.DECIDED) ^
               !Arrays.equals(this.value, value) : view + " " + value + " " + this;

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

        if (state != LogEntryState.DECIDED) {
            state = LogEntryState.KNOWN;
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

    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public int hashCode() {
        return super.hashCode();
    }

    private static final long serialVersionUID = 1L;
}

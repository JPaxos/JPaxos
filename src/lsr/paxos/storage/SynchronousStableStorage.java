package lsr.paxos.storage;

import java.io.IOException;

import lsr.paxos.Snapshot;

public class SynchronousStableStorage extends UnstableStorage {
    public final DiscWriter writer;

    public SynchronousStableStorage(DiscWriter writer) throws IOException {
        view = writer.loadViewNumber();
        this.writer = writer;

        // Synchronous log reads the previous log files
        log = new SynchronousLog(writer);

        Snapshot snapshot = this.writer.getSnapshot();
        if (snapshot != null) {
            super.setLastSnapshot(snapshot);
        }
    }

    @Override
    public void setView(int view) throws IllegalArgumentException {
        writer.changeViewNumber(view);
        super.setView(view);
    }

    @Override
    public void setLastSnapshot(Snapshot snapshot) {
        writer.newSnapshot(snapshot);
        super.setLastSnapshot(snapshot);
    }
}

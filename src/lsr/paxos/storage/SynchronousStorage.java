package lsr.paxos.storage;

import java.io.IOException;

import lsr.common.CrashModel;
import lsr.common.ProcessDescriptor;
import lsr.paxos.Snapshot;

/**
 * Implementation of <code>Storage</code> interface. This implementation is
 * using <code>DiscWriter</code> to save view number and last snapshot to disc.
 * It is also using synchronous version of log - <code>SynchronousLog</code>.
 * 
 * @see SynchronousLog
 * @see DiscWriter
 */
public class SynchronousStorage extends InMemoryStorage {
    private final DiscWriter writer;

    /**
     * Initializes new instance of <code>SynchronousStorage</code> class.
     * 
     * @param writer - the disc writer
     * @throws IOException if I/O error occurs
     */
    public SynchronousStorage(DiscWriter writer) throws IOException {
        assert CrashModel.FullSS.equals(ProcessDescriptor.processDescriptor.crashModel);

        view = writer.loadViewNumber();
        this.writer = writer;

        // synchronous log reads the previous log files
        log = new SynchronousLog(writer);

        Snapshot snapshot = this.writer.getSnapshot();
        if (snapshot != null) {
            super.setLastSnapshot(snapshot);
        }
    }

    public void setView(int view) {
        writer.changeViewNumber(view);
        super.setView(view);
    }

    public void setLastSnapshot(Snapshot snapshot) {
        writer.newSnapshot(snapshot);
        super.setLastSnapshot(snapshot);
    }
}

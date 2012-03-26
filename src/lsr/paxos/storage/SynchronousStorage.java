package lsr.paxos.storage;

import java.io.IOException;

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
        view = writer.loadViewNumber();
        this.writer = writer;

        // synchronous log reads the previous log files
        log = new SynchronousLog(writer);

    }

    public void setView(int view) {
        writer.changeViewNumber(view);
        super.setView(view);
    }

}

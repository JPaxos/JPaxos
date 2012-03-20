package lsr.paxos.storage;

/**
 * Represents the storage where the view number is saved to stable storage every
 * time it changes. It is using <code>SingleNumberWriter</code> to read and
 * write the from stable storage.
 */
public class SynchronousViewStorage extends InMemoryStorage {
    private final SingleNumberWriter writer;

    /**
     * Creates new storage with synchronous writes per view change.
     * 
     * @param writer - used to read and write the view number to disc
     */
    public SynchronousViewStorage(SingleNumberWriter writer) {
        this.writer = writer;
        this.view = (int) writer.readNumber();
    }

    public void setView(int view) throws IllegalArgumentException {
        writer.writeNumber(view);
        super.setView(view);
    }
}

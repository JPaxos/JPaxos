package lsr.paxos.storage;

import java.io.IOException;
import java.util.Collection;

import lsr.paxos.Snapshot;

public interface DiscWriter {

    /* Synchronous */

    void changeInstanceView(int instanceId, int view);

    void changeInstanceValue(int instanceId, int view, byte[] value);

    void changeViewNumber(int view);

    void close() throws IOException;

    Collection<ConsensusInstance> load() throws IOException;

    int loadViewNumber() throws IOException;

    void newSnapshot(Snapshot snapshot);

    Snapshot getSnapshot();

    /* Asynchronous (but must be written before/with next synchronous) */

    void decideInstance(int instanceId);

}
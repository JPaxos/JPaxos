package lsr.paxos.storage;

import java.io.IOException;
import java.util.Collection;

public interface DiscWriter {

    /* Synchronous */

    void changeInstanceView(int instanceId, int view);

    void changeInstanceValue(int instanceId, int view, byte[] value);

    void changeViewNumber(int view);

    void close() throws IOException;

    Collection<ConsensusInstance> load() throws IOException;

    int loadViewNumber() throws IOException;

    /* Asynchronous (but must be written before/with next synchronous) */

    void decideInstance(int instanceId);

}
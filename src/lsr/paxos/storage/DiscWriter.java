package lsr.paxos.storage;

import java.io.IOException;
import java.util.Collection;

import lsr.common.Pair;

public interface DiscWriter {

	void changeInstanceView(int instanceId, int view);

	void changeInstanceValue(int instanceId, int view, byte[] value);

	/** ↓ Asynchronous **/
	void decideInstance(int instanceId);

	void changeViewNumber(int view);

	void close() throws IOException;

	Collection<ConsensusInstance> load() throws IOException;

	int loadViewNumber() throws IOException;

	/** synchronous method for convenience (unused in JPaxos) */

	void record(Object key, Object value);

	Object retrive(Object key);

	void newSnapshot(Pair<Integer, byte[]> snapshot);

	Pair<Integer, byte[]> getSnapshot();

}
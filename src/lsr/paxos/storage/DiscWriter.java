package lsr.paxos.storage;

import java.io.IOException;
import java.util.Collection;

public interface DiscWriter {

	void changeInstanceView(int instanceId, int view);

	void changeInstanceValue(int instanceId, int view, byte[] value);
	
	/**   ↓ Asynchronous **/
	void decideInstance(int instanceId);

	void changeViewNumber(int view);

	void close() throws IOException;

	Collection<ConsensusInstance> load() throws IOException;

	int loadViewNumber() throws IOException;

}
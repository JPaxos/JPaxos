package put.consensus;

import java.io.IOException;

import put.consensus.listeners.ConsensusListener;

public interface Consensus extends Storage {
	/**
	 * Blocking method. Gets the object proposed and decided.
	 */
	void propose(Object obj);

	void start() throws IOException;

	void addConsensusListener(ConsensusListener listener);

	void removeConsensusListener(ConsensusListener listener);
}

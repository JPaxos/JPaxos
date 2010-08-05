package put.consensus.listeners;

public interface ConsensusListener {
	/**
	 * Executed when a decision has been taken. Includes decisions proposed by
	 * self.
	 */
	void decide(Object obj);
}

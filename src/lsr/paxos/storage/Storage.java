package lsr.paxos.storage;

import java.util.BitSet;
import java.util.List;

import lsr.common.PID;

public interface Storage {
	void updateFirstUncommitted();

	/**
	 * First consensus instance for which there is yet no decision. That is,
	 * <code>topDecided+1</code> is the first instance for which this process
	 * doesn't know the decision.
	 */
	int getFirstUncommitted();

	List<PID> getProcesses();

	/** Number of processes */
	int getN();

	int getLocalId();

	BitSet getAcceptors();

	BitSet getLearners();

	StableStorage getStableStorage();

	void setWindowSize(int windowSize);

	int getWindowSize();

	boolean isInWindow(int instanceId);

	Log getLog();
}

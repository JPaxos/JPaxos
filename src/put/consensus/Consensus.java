package put.consensus;

import java.io.IOException;

import put.consensus.listeners.ConsensusListener;

public interface Consensus extends Storage {
    /**
     * Blocking method. Gets the object proposed and decided. Executed on
     * default ConsensusDelegateProposer.
     */
    void propose(Object obj);

    ConsensusDelegateProposer getNewDelegateProposer() throws IOException;

    void start() throws IOException;

    void addConsensusListener(ConsensusListener listener);

    void removeConsensusListener(ConsensusListener listener);
}

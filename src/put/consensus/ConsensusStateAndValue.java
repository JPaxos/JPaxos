package put.consensus;

import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * Place holder for both state and value.
 * 
 * If the state is unknown the object is never written (i.e. it's a null
 * reference).
 * 
 * @author Jan K
 */

@Deprecated
public class ConsensusStateAndValue {
    /** Instance state */
    public LogEntryState state = LogEntryState.UNKNOWN;
    /** Instance value - the exact object that has been proposed */
    public Object value = null;
}
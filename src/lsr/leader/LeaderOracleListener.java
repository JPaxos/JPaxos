package lsr.leader;

/**
 * Interface that should be implemented by classes interested in receiving
 * notifications of changes on the leader.
 * 
 * @author Nuno
 */
public interface LeaderOracleListener {
    /**
     * Called when the leader changes.
     * 
     * @param leader the id of the new leader
     */
    public void onNewLeaderElected(int leader);
}

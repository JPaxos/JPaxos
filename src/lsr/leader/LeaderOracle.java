package lsr.leader;


/**
 * Methods that all leader election implementations should provide.
 * 
 * @author Nuno Santos (LSR)
 */
public interface LeaderOracle {
	/** Returns the current leader. */
	public int getLeader();
	
	/** Activates the leader oracle. 
	 * @throws Exception */
	public void start() throws Exception; 
	
	/** Stop the leader oracle activities 
	 * @throws Exception */
	public void stop() throws Exception;

	/** Registers a listener for leader election events. */
	public void registerLeaderOracleListener(LeaderOracleListener listener);
	
	/** Removes a listener for leader election events. */
	public void removeLeaderOracleListener(LeaderOracleListener listener);

	public int getDelta();
	
}

package process_handlers;

/**
 * Constructor MUST start the replica!!!
 */

public interface ReplicaHandler extends ProcessHandler {
	int getLocalId();
}

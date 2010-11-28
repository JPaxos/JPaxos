package lsr.paxos.replica;

import lsr.common.ClientCommand;

/**
 * This <code>CommandCallback</code> interface provide method of executing
 * command received from clients.
 * 
 */
public interface CommandCallback {
    /**
     * Executes command received from specified client.
     * 
     * @param command - received client command
     * @param client - client which request this command
     * @see ClientCommand, ClientProxy
     */
    void execute(ClientCommand command, ClientProxy client);
}

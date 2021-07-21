package lsr.paxos.replica;

import lsr.common.ClientReply;

/**
 * Handles the reply produced by the Service, passing it to client
 */
public interface ClientProxy {
    /** Called upon generating the answer for previous request */
    void send(ClientReply clientReply);

    /**
     * Called to get rid of a client, commanding it to connect to another
     * replica
     * 
     * @return true if client gone; false if failed to get rid of it
     */
    boolean redirectElsewhere();
}

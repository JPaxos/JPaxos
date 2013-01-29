package lsr.paxos.replica;

import lsr.common.ClientReply;

/**
 * Handles the reply produced by the Service, passing it to client
 */
public interface ClientProxy {
    /** Called upon generating the answer for previous request */
    void send(ClientReply clientReply);
}

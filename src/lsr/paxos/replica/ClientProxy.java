package lsr.paxos.replica;

import java.io.IOException;

import lsr.common.ClientReply;

public interface ClientProxy {

	/**
	 * Sends the message to the client.
	 * 
	 * @param clientReply
	 *            - message to send
	 * @throws IOException
	 */
	public abstract void send(ClientReply clientReply) throws IOException;
}
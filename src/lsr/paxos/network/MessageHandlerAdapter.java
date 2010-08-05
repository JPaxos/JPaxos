package lsr.paxos.network;

import java.util.BitSet;

import lsr.paxos.messages.Message;

public class MessageHandlerAdapter implements MessageHandler {

	public void onMessageReceived(Message msg, int sender) {
	}

	public void onMessageSent(Message message, BitSet destinations) {
	}
}

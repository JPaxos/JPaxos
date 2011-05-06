package lsr.paxos.recovery;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;

public class MockNetwork extends Network {
    private Map<Integer, Message> messages;
    private List<Message> messagesSentToAll;

    public MockNetwork() {
        messages = new HashMap<Integer, Message>();
        messagesSentToAll = new ArrayList<Message>();
    }

    public void sendMessage(Message message, int destination) {
        BitSet destinations = new BitSet();
        destinations.set(destination);
        sendMessage(message, destinations);
    }

    public void sendMessage(Message message, BitSet destination) {
        for (int i = destination.nextSetBit(0); i >= 0; i = destination.nextSetBit(i + 1)) {
            messages.put(i, message);
        }

        fireSentMessage(message, destination);
    }

    public void sendToAll(Message message) {
        BitSet destinations = new BitSet();
        destinations.set(0, ProcessDescriptor.getInstance().numReplicas);
        sendMessage(message, destinations);

        messagesSentToAll.add(message);
    }

    public Map<Integer, Message> getSentMessages() {
        return messages;
    }

    public List<Message> getMessagesSentToAll() {
        return messagesSentToAll;
    }

    public boolean fireReceive(Message message, int sender) {
        fireReceiveMessage(message, sender);
        return true;
    }

    public void start() {
    }
}
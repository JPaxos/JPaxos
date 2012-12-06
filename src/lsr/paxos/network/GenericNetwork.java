package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;
import java.util.logging.Logger;

import lsr.common.PID;
import lsr.paxos.messages.Message;

public class GenericNetwork extends Network {
    private final UdpNetwork udpNetwork;
    private final TcpNetwork tcpNetwork;
    private final PID[] processes;

    public GenericNetwork(TcpNetwork tcpNetwork, UdpNetwork udpNetwork) {
        processes = processDescriptor.config.getProcesses().toArray(new PID[0]);

        this.tcpNetwork = tcpNetwork;
        this.udpNetwork = udpNetwork;
    }

    @Override
    public void start() {
        udpNetwork.start();
        tcpNetwork.start();
    }

    // we using internal methods in networks, so listeners has to be handled
    public void sendMessage(Message message, BitSet destinations) {
        assert !destinations.isEmpty() : "Sending a message to noone";

        BitSet dests = (BitSet) destinations.clone();
        if (dests.get(processDescriptor.localId)) {
            fireReceiveMessage(message, processDescriptor.localId);
            dests.clear(processDescriptor.localId);
        }

        // serialize message to discover its size
        byte[] data = message.toByteArray();

        // send message using UDP or TCP
        if (data.length < processDescriptor.maxUdpPacketSize) {
            // packet small enough to send using UDP
            udpNetwork.send(data, dests);
        } else {
            // big packet so send using TCP
            for (int i = dests.nextSetBit(0); i >= 0; i = dests.nextSetBit(i + 1)) {
                tcpNetwork.send(data, i);
            }
        }

        fireSentMessage(message, destinations);
    }

    public void sendMessage(Message message, int destination) {
        BitSet target = new BitSet();
        target.set(destination);
        sendMessage(message, target);
    }

    public void sendToAll(Message message) {
        BitSet all = new BitSet(processes.length);
        all.set(0, processes.length);
        sendMessage(message, all);
    }

    @SuppressWarnings("unused")
    private final static Logger _logger = Logger.getLogger(GenericNetwork.class.getCanonicalName());

    @Override
    public boolean send(byte[] message, int destination) {
        throw new UnsupportedOperationException();
    }
}

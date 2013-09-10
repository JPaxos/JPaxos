package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;

import lsr.paxos.messages.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericNetwork extends Network {
    private final UdpNetwork udpNetwork;
    private final TcpNetwork tcpNetwork;

    public GenericNetwork(TcpNetwork tcpNetwork, UdpNetwork udpNetwork) {
        this.tcpNetwork = tcpNetwork;
        this.udpNetwork = udpNetwork;
    }

    @Override
    public void start() {
        udpNetwork.start();
        tcpNetwork.start();
    }

    // we using internal methods in networks, so listeners has to be handled
    public void send(Message message, BitSet destinations) {
        // send message using UDP or TCP
        if (message.byteSize() < processDescriptor.maxUdpPacketSize) {
            // packet small enough to send using UDP
            udpNetwork.sendMessage(message, destinations);
        } else {
            // big packet so send using TCP
            tcpNetwork.sendMessage(message, destinations);
        }
    }

    @Override
    protected void send(Message message, int destination) {
        // send message using UDP or TCP
        if (message.byteSize() < processDescriptor.maxUdpPacketSize) {
            // packet small enough to send using UDP
            udpNetwork.send(message, destination);
        } else {
            // big packet so send using TCP
            tcpNetwork.send(message, destination);
        }
    }

    @SuppressWarnings("unused")
    private final static Logger logger = LoggerFactory.getLogger(GenericNetwork.class);
}

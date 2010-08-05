package lsr.paxos.network;

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;
import lsr.paxos.messages.MessageType;

public class GenericNetwork extends AbstractNetwork {
	private final UdpNetwork _udpNetwork;
	private final TcpNetwork _tcpNetwork;
	private final InnerMessageHandler _innerListener;
	private final PID[] _processes;
	private final ProcessDescriptor p;

	public GenericNetwork(ProcessDescriptor p, TcpNetwork tcpNetwork,
			UdpNetwork udpNetwork) {
		this.p = p;
		_processes = p.config.getProcesses().toArray(new PID[0]);
		_innerListener = new InnerMessageHandler();

		_tcpNetwork = tcpNetwork;
		_tcpNetwork.addMessageListener(MessageType.ANY, _innerListener);

		_udpNetwork = udpNetwork;
		_udpNetwork.addMessageListener(MessageType.ANY, _innerListener);
	}

	public void sendMessage(Message message, BitSet destinations) {
		if (_logger.isLoggable(Level.FINE)) {
			_logger.fine("Sending " + message + " to " + destinations);
		}
		BitSet dests = (BitSet) destinations.clone();
		// we using internal methods in networks, so listener has to be handled
		if (dests.get(p.localID)) {
			fireReceiveMessage(message, p.localID);
			dests.clear(p.localID);
		}

		// serialize message to discover its size
		byte[] data = MessageFactory.serialize(message);

		// send message using UDP or TCP
		// if (data.length < Config.MAX_UDP_PACKET_SIZE) {
		if (data.length < p.maxUdpPacketSize) {
			// packet small enough to send using UDP
			_udpNetwork.send(data, dests);
		} else {
			// big packet so send using TCP
			for (int i = dests.nextSetBit(0); i >= 0; i = dests
					.nextSetBit(i + 1)) {
				if (i != p.localID)
					_tcpNetwork.send(data, i);
			}
		}

		fireSentMessage(message, dests);
	}

	public void sendMessage(Message message, int destination) {
		BitSet all = new BitSet();
		all.set(destination);
		sendMessage(message, all);
	}

	public void sendToAll(Message message) {
		BitSet all = new BitSet(_processes.length);
		all.set(0, _processes.length);
		sendMessage(message, all);
	}

	private class InnerMessageHandler implements MessageHandler {
		public void onMessageReceived(Message msg, int sender) {
			fireReceiveMessage(msg, sender);
		}

		public void onMessageSent(Message message, BitSet destinations) {
			fireSentMessage(message, destinations);

		}
	}

	private final static Logger _logger = Logger.getLogger(GenericNetwork.class
			.getCanonicalName());
}

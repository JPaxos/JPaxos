package lsr.paxos.network;

import java.util.BitSet;
import java.util.logging.Logger;

import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

public class GenericNetwork extends Network {
	private final UdpNetwork _udpNetwork;
	private final TcpNetwork _tcpNetwork;
	private final PID[] _processes;
	private final ProcessDescriptor pDesc;

	public GenericNetwork(TcpNetwork tcpNetwork, UdpNetwork udpNetwork) {
		pDesc = ProcessDescriptor.getInstance();
		_processes = pDesc.config.getProcesses().toArray(new PID[0]);

		_tcpNetwork = tcpNetwork;
		_udpNetwork = udpNetwork;
	}

	// we using internal methods in networks, so listeners has to be handled
	public void sendMessage(Message message, BitSet destinations) {
		assert !destinations.isEmpty() : "Sending a message to noone";

		BitSet dests = (BitSet) destinations.clone();
		if (dests.get(pDesc.localID)) {
			fireReceiveMessage(message, pDesc.localID);
			dests.clear(pDesc.localID);
		}

		// serialize message to discover its size
		byte[] data = MessageFactory.serialize(message);

		// send message using UDP or TCP
		if (data.length < pDesc.maxUdpPacketSize) {
			// packet small enough to send using UDP
			_udpNetwork.send(data, dests);
		} else {
			// big packet so send using TCP
			for (int i = dests.nextSetBit(0); i >= 0; i = dests.nextSetBit(i + 1))
				_tcpNetwork.send(data, i);
		}

		fireSentMessage(message, destinations);
	}

	public void sendMessage(Message message, int destination) {
		BitSet target = new BitSet();
		target.set(destination);
		sendMessage(message, target);
	}

	public void sendToAll(Message message) {
		BitSet all = new BitSet(_processes.length);
		all.set(0, _processes.length);
		sendMessage(message, all);
	}

	@SuppressWarnings("unused")
	private final static Logger _logger = Logger.getLogger(GenericNetwork.class.getCanonicalName());
}

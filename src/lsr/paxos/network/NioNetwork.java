package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import lsr.paxos.messages.Message;

public class NioNetwork extends Network implements Runnable {
	private final static Logger logger = Logger.getLogger(Network.class
			.getCanonicalName());
	
	private Selector selector;

	// input, output
	NioConnection[][] connections;

	private HashMap<SocketChannel, ByteBuffer> tmpBuffers = new HashMap<SocketChannel, ByteBuffer>();

	public NioNetwork()
			throws IOException {

		selector = SelectorProvider.provider().openSelector();

		ServerSocketChannel serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);
		serverChannel.socket().bind(new InetSocketAddress((InetAddress) null,
                processDescriptor.getLocalProcess().getReplicaPort()));
		serverChannel.register(selector, SelectionKey.OP_ACCEPT);

		// input, output
		connections = new NioConnection[processDescriptor.numReplicas][2];
		//for (int i = localId + 1; i < processDescriptor.numReplicas; i++) {
		for (int i = 0; i < processDescriptor.numReplicas; i++) {
		    if (i != localId)
		    {
		        connections[i][1] = new NioOutputConnection(this,  processDescriptor.config.getProcess(i), null);
		        if (i > localId)
		            connections[i][1].start();
		    }
		}
	}

	@Override
	public void start() {
		Thread t = new Thread(this);
		t.setDaemon(true);
		t.start();
	}

	@Override
	public void run() {
		while (true) {
			try {
				selector.select();

				Iterator<SelectionKey> selectedKeys = this.selector
						.selectedKeys().iterator();
				while (selectedKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectedKeys.next();
					selectedKeys.remove();

					if (!key.isValid())
						continue;

					if (key.isAcceptable())
						accept(key);
					if (key.isReadable())
						read(key);
				}
			} catch (ClosedChannelException e) {
				logger.finest("other process closed a channel when attepting to connect here");
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	protected void accept(SelectionKey key) throws IOException,
			ClosedChannelException {
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
				.channel();
		SocketChannel channel = serverSocketChannel.accept();
		configureChannel(channel);

		tmpBuffers.put(channel, ByteBuffer.allocate(8));
		channel.register(selector, SelectionKey.OP_READ);

		logger.finest("socket accept: " + channel);
	}

	protected void configureChannel(SocketChannel channel) throws IOException,
			SocketException {
		channel.configureBlocking(false);
		channel.socket().setTcpNoDelay(true);
		channel.socket().setSendBufferSize(NioConnection.TCP_BUFFER_SIZE);
		channel.socket().setReceiveBufferSize(NioConnection.TCP_BUFFER_SIZE);
	}

	private void read(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();
		ByteBuffer buffer = tmpBuffers.get(channel);

		logger.finest("socket read: " + channel + " "
				+ (tmpBuffers.get(channel) != null));

		int numRead = 0;
		try {
			numRead = channel.read(buffer);
		} catch (IOException e) {
			logger.finest("client disconnected");
			key.cancel();
			tmpBuffers.remove(channel);
			return;
		}

		if (numRead == -1) {
			logger.finest("client disconnected");
			key.cancel();
			tmpBuffers.remove(channel);
			return;
		}

		if (buffer.position() != buffer.capacity())
			return;

		buffer.flip();

		buffer.getInt();
		int senderId = buffer.getInt();
		key.cancel();
		tmpBuffers.remove(channel);

		if (senderId >= processDescriptor.numReplicas)
		{
			logger.warning("Invalid senderId: " + senderId);
			return;
		}

		NioInputConnection inputConnection = new NioInputConnection(this, processDescriptor.config.getProcess(senderId), 
				channel);
		connections[senderId][0] = inputConnection;
		inputConnection.start();

		NioOutputConnection outputConnection = new NioOutputConnection(this, processDescriptor.config.getProcess(senderId),
				channel);
		connections[senderId][1] = outputConnection;
		outputConnection.start();
		//
		//
		// ((NioOutputConnection) connections[senderId][1])
		// .notifyAboutInputConnected();

		logger.fine("input connection established with: " + senderId);
		// logger.fine("output connection established to: " + receiverId);
	}

	@Override
	protected void send(Message message, int destination) {
		boolean sent = ((NioOutputConnection) connections[destination][1])
				.send(message.toByteArray());

		logger.finest("send message to: " + destination + " - "
				+ (sent == true ? "submitted" : "rejected"));
	}

	@Override
	protected void send(Message message, BitSet destinations) {
		for (int i = destinations.nextSetBit(0); i >= 0; i = destinations
				.nextSetBit(i + 1)) {
			send(message, i);
		}
	}

	protected void removeConnection(int senderId, int receiverId) {
		logger.finest("connection closed with: " + senderId + " --> "
				+ receiverId);
		if (receiverId == localId)
			((NioOutputConnection) connections[senderId][1])
					.notifyAboutInputDisconnected();
	}
}

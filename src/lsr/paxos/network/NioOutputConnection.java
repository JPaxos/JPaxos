package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import lsr.common.PID;

public class NioOutputConnection extends NioConnection {
	private final static Logger logger = Logger.getLogger(Network.class
			.getCanonicalName());

	private static final int MAX_PENDING_DATA = 1024;
	private static final int DRAIN_LIMIT = 64;
	private static final int BUFFER_SIZE = 8192;

	private BlockingQueue<byte[]> pendingData = new ArrayBlockingQueue<byte[]>(
			MAX_PENDING_DATA);
	private ArrayDeque<byte[]> drained = new ArrayDeque<byte[]>(DRAIN_LIMIT);

	ByteBuffer outputBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

	byte[] currentData;
	int currentDataWritten = 0;

	private volatile boolean connected = false;

	public NioOutputConnection(NioNetwork network, PID replica, SocketChannel channel) throws IOException {
		super(network, replica, channel);
		if (channel != null) {
			channel.register(selector, SelectionKey.OP_WRITE);
			connected = true;
			outputBuffer.flip();
			logger.fine("output connection established to: " + replica.getId());
		}
		
//		if (network.localId == 0)
//			pendingData = new ArrayBlockingQueue<byte[]>(100 * 1024);
	}

	public boolean send(byte[] data) {
		// if (!connected)
		// return false;

		try {
			boolean success = pendingData.offer(data);
			if (!success) {
				//long time = System.currentTimeMillis();
				pendingData.put(data);
				//Network.writeWaitTime.add(data[4], System.currentTimeMillis() - time);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	protected void tryConnect() throws IOException {
		logger.fine("trying to connect to: " + replica.getId());

		channel = SocketChannel.open();
		network.configureChannel(channel);
		channel.connect(new InetSocketAddress(replica.getHostname(),
                replica.getReplicaPort()));
		
		channel.register(selector, SelectionKey.OP_CONNECT);
	}

	protected void introduce() {
		logger.finer("introducing myself to: " + replica.getId());

		// ByteBuffer myIdByteBuffer = ByteBuffer.allocate(4);
		// myIdByteBuffer.putInt(network.localId);
		// myIdByteBuffer.flip();

		outputBuffer.clear();
		outputBuffer.putInt(4);
		outputBuffer.putInt(Network.localId);
		outputBuffer.flip();

		// selector.wakeup();
		// pendingData.offer(myIdByteBuffer);
	}

	@Override
	public void run() {
		setName("NioOutputConnection_" + Network.localId + "->" + replica.getId());
		logger.finest("starting");
		while (true) {
			try {
				if (!connected) {
					disposeConnection();
					if (isActiveConnection())
						tryConnect();
					else
						return;
				} else {
					if (outputBuffer.remaining() == 0)
						fillBuffer();
				}

				selector.select();

				Iterator<SelectionKey> selectedKeys = selector.selectedKeys()
						.iterator();
				while (selectedKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectedKeys.next();
					selectedKeys.remove();

					if (!key.isValid())
						continue;

					if (key.isConnectable())
						finishConnect(key);
					if (key.isWritable())
						write(key);
					// if (key.isReadable())
					// disposeConnection();
				}
			} catch (Exception e) {
				logger.finest("output connection problem with: " + replica.getId());
				// e.printStackTrace();
				connected = false;
			}
			if (!connected && isActiveConnection()) {
				try {
					sleep(processDescriptor.tcpReconnectTimeout);
				} catch (InterruptedException e) {
					logger.finest("waking up early to try to connect");
				}
			}
		}
	}

	private void fillBuffer() throws InterruptedException {
		logger.finer("filling buffer");
		outputBuffer.clear();
		while (true) {
			if (currentData != null) {
				logger.finer("copying data to buffer");
				int bytesToCopy = Math.min(currentData.length
						- currentDataWritten, outputBuffer.remaining());
				outputBuffer.put(currentData, currentDataWritten, bytesToCopy);
				currentDataWritten += bytesToCopy;
				if (currentDataWritten == currentData.length) {
					logger.finer("currentData written entirely");
					currentData = null;
				}
			}
			if (outputBuffer.remaining() < 4
					|| (outputBuffer.position() != 0 && noPendingData())) {
				logger.finer("preparing buffer for sending");
				outputBuffer.flip();
				return;
			}
			if (currentData == null) {
				logger.finer("taking new data from the queue");
				currentData = getNewData();
				currentDataWritten = 0;
				outputBuffer.putInt(currentData.length);
			}
		}
	}

	private boolean noPendingData() {
		// return pendingData.isEmpty();
		if (!drained.isEmpty())
			return false;
		pendingData.drainTo(drained, DRAIN_LIMIT);
		return drained.isEmpty();
	}

	private byte[] getNewData() throws InterruptedException {
		// return pendingData.take();

		if (!drained.isEmpty())
			return drained.removeFirst();
		else {
			pendingData.drainTo(drained, DRAIN_LIMIT);
			if (!drained.isEmpty())
				return drained.removeFirst();
			else {
				return pendingData.take();
			}
		}
	}

	private void write(SelectionKey key) throws IOException {
		logger.finest("writing to: " + replica.getId());

		int count = channel.write(outputBuffer);
		
		//Network.bytesWritten.add(replica.getId(), count);
		//Network.numberOfWrites.increment(replica.getId());

		logger.finer("writing to: " + replica.getId() + " (" + count + " bytes)");
	}

	private void finishConnect(SelectionKey key) throws IOException {
		channel.finishConnect();
		connected = true;
		key.interestOps(SelectionKey.OP_WRITE);

		logger.fine("output connection established to: " + replica.getId()
				+ " (finishConnect)");

		NioInputConnection inputConnection = new NioInputConnection(network,
				replica, channel);
		network.connections[replica.getId()][0] = inputConnection;
		inputConnection.start();

		introduce();
	}

	public void notifyAboutInputConnected() {
		if (!connected) {
			logger.fine("got notification about input established, trying to establish output: "
					+ replica.getId());
			this.interrupt();
		}
	}

	public void notifyAboutInputDisconnected() {
		connected = false;
		this.interrupt();
		logger.fine("got notification about input disconnected, disconnecting myself: "
				+ replica.getId());
	}

	protected void disposeConnection() {
		connected = false;
		currentData = null;
		for (SelectionKey key : selector.keys())
			key.cancel();
		pendingData.clear();

		logger.fine("output connection closed with: " + replica.getId());

		network.removeConnection(Network.localId, replica.getId());
	}
}

package lsr.common.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * This class provides default implementation of <code>ReadWriteHandler</code>
 * using java channels. It provides method used to send byte array, which will
 * be send as soon as there will be space in system send buffer. Reading data is
 * done using <code>PacketHandler</code>. After setting new
 * <code>PacketHandler</code> to this object, reading mode is enabled, and reads
 * data to fill entire byte buffer(provided by <code>PacketHandler</code>). If
 * no space remain available in read buffer, <code>PacketHandler</code> is
 * notified by calling <code>finish</code> method on it. The handler is removed
 * after reading whole packet, so it has to be set again.
 * 
 * @see PacketHandler
 */
public class ReaderAndWriter implements ReadWriteHandler {
	public SelectorThread _selectorThread;
	public SocketChannel _socketChannel;
	public Queue<byte[]> _messages;
	public PacketHandler _packetHandler;
	public ByteBuffer _writeBuffer;

	/**
	 * Creates new <code>ReaderAndWrite</code> using socket channel and selector
	 * thread. It will also register this socket channel into selector.
	 * 
	 * @param socketChannel
	 *            - channel used to read and write data
	 * @param selectorThread
	 *            - selector which will handle all operations from this channel
	 * @throws IOException
	 *             - if registering channel to selector has failed
	 */
	public ReaderAndWriter(SocketChannel socketChannel,
			SelectorThread selectorThread) throws IOException {
		_socketChannel = socketChannel;
		_selectorThread = selectorThread;
		_messages = new LinkedList<byte[]>();
		_selectorThread.registerChannel(socketChannel, 0, this);
	}

	/**
	 * Registers new packet handler. All received data will be written into its
	 * buffer. The reading will be activated on underlying channel.
	 * 
	 * @param packetHandler
	 *            the packet handler to set
	 */
	public void setPacketHandler(PacketHandler packetHandler) {
		assert _packetHandler == null : "Previous packet wasn't read yet.";
		_packetHandler = packetHandler;
		_selectorThread.scheduleAddChannelInterest(_socketChannel,
				SelectionKey.OP_READ);
	}

	/**
	 * This method is called from selector thread to notify that there are new
	 * data available in socket channel.
	 */
	public void handleRead() {
		try {
			while (_packetHandler != null) {
				int readBytes = _socketChannel.read(_packetHandler
						.getByteBuffer());

				// no more data in system buffer
				if (readBytes == 0)
					break;

				// EOF - that means that the other side close his socket, so we
				// should close this connection too.
				if (readBytes == -1) {
					innerClose();
					return;
				}

				// if the whole packet was read, then notify packet handler;
				// calling return instead of break cause that the OP_READ flag
				// is not set ; to start reading again, new packet handler has
				// to be set
				if (_packetHandler.getByteBuffer().remaining() == 0) {
					PacketHandler old = _packetHandler;
					_packetHandler = null;
					old.finished();
					return;
				}
				break;
			}
		} catch (IOException e) {
			innerClose();
			return;
		}
		_selectorThread
				.addChannelInterest(_socketChannel, SelectionKey.OP_READ);
	}

	/**
	 * This method is called from selector thread to notify that there is free
	 * space in system send buffer, and it is possible to send new packet of
	 * data.
	 */
	public void handleWrite() {
		synchronized (_messages) {
			// try to send all messages
			while (!_messages.isEmpty()) {
				// create buffer from first message
				if (_writeBuffer == null)
					_writeBuffer = ByteBuffer.wrap(_messages.peek());

				// write as many bytes as possible
				int writeBytes = 0;
				try {
					writeBytes = _socketChannel.write(_writeBuffer);
				} catch (IOException e) {
					e.printStackTrace();
					innerClose();
					return;
				}

				// cannot write more so break
				if (writeBytes == 0)
					break;

				// remove message after sending
				if (_writeBuffer.remaining() == 0) {
					_writeBuffer = null;
					_messages.poll();
				}
			}
			// if there are messages to send, add interest in writing
			if (!_messages.isEmpty())
				_selectorThread.addChannelInterest(_socketChannel,
						SelectionKey.OP_WRITE);
		}
	}

	/**
	 * Adds the message to the queue of messages to sent. This method is
	 * asynchronous and will return immediately.
	 * 
	 * @param message
	 */
	public void send(byte[] message) {
		// discard message if channel is not connected
		if (!_socketChannel.isConnected())
			return;

		synchronized (_messages) {
			_messages.add(message);

			// if writing is not active, activate it
			if (_writeBuffer == null)
				_selectorThread.scheduleAddChannelInterest(_socketChannel,
						SelectionKey.OP_WRITE);
		}
	}

	/**
	 * Closes the underlying socket channel.
	 */
	public void close() {
		_selectorThread.beginInvoke(new Runnable() {
			public void run() {
				innerClose();
			}
		});
	}

	/**
	 * Closes the underlying socket channel. It closes channel immediately so it
	 * should be called only from selector thread.
	 */
	private void innerClose() {
		try {
			_socketChannel.close();
		} catch (IOException e) {
			// when the closing channel can throw an exception?
			// e.printStackTrace();
			throw new RuntimeException(
					"\"when the closing channel can throw an exception?\" Take a look, here!",
					e);
		}
	}
	
	@SuppressWarnings("unused")
	private final static Logger _logger = Logger.getLogger(ReaderAndWriter.class.getCanonicalName());
}
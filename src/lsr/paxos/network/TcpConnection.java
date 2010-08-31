package lsr.paxos.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Config;
import lsr.common.KillOnExceptionHandler;
import lsr.common.PID;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

/**
 * This class is responsible for handling stable TCP connection to other
 * replica, provides to method for establishing new connection: active and
 * passive. In active mode we try to connect to other side creating new socket
 * and connects. If passive mode is enabled, then we wait for socket from the
 * <code>SocketServer</code> provided by <code>TcpNetwork</code>.
 * <p>
 * Every time new message is received from this connection, it is deserialized,
 * and then all registered network listeners in related <code>TcpNetwork</code>
 * are notified about it.
 */
public class TcpConnection {
	private Socket _socket;
	private DataInputStream _input;
	private DataOutputStream _output;
	private final PID _replica;
	private boolean _connected = false;
	/** true if connection should be started by this replica; */
	private final boolean _active;
	private int _localId;
	private final TcpNetwork _network;

	private final Thread senderThread;
	private final Thread receiverThread;

	private final ArrayBlockingQueue<byte[]> sendQueue = new ArrayBlockingQueue<byte[]>(
			128);

	private TcpConnection(TcpNetwork network, PID replica, boolean active) {
		_network = network;
		_replica = replica;
		_active = active;

		this.receiverThread = new Thread(new ReceiverThread(), "TcpReceiver"
				+ _replica.getId());
		this.senderThread = new Thread(new Sender(), "TcpSender"
				+ _replica.getId());
		receiverThread
				.setUncaughtExceptionHandler(new KillOnExceptionHandler());
		senderThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
	}

	/**
	 * Creates new <b>active</b> TCP connection to specified replica.
	 * 
	 * @param network
	 *            - related <code>TcpNetwork</code>
	 * @param replica
	 *            - replica to connect to
	 * @param localId
	 *            - the id of this replica
	 */
	public TcpConnection(TcpNetwork network, PID replica, int localId) {
		this(network, replica, true);
		_localId = localId;
	}

	/**
	 * Creates new <b>passive</b> TCP connection to specified replica.
	 * 
	 * @param network
	 *            - related <code>TcpNetwork</code>
	 * @param replica
	 *            - replica to connect to
	 */
	public TcpConnection(TcpNetwork network, PID replica) {
		this(network, replica, false);
	}

	public synchronized void start() {
		receiverThread.start();
		senderThread.start();
	}

	final class Sender implements Runnable {
		@Override
		public void run() {
			_logger.info("Sender thread started");
			try {
				// Wait for connection to be established.
				synchronized (TcpConnection.this) {
					while (!_connected) {
						TcpConnection.this.wait();
					}
				}

				while (true) {
					byte[] msg = sendQueue.take();
					try {
						_output.write(msg);
						_output.flush();
					} catch (IOException e) {
						_logger.log(Level.WARNING, "Error sending message", e);
					}
					// _logger.info("QS:" + sendQueue.size() + " Msg: " +
					// msgDesc.message);
				}
			} catch (InterruptedException e) {
				_logger.log(Level.SEVERE, "Fatal error", e);
				System.exit(1);
			}
		}
	}

	/**
	 * Main loop used to connect and read from the socket.
	 */
	final class ReceiverThread implements Runnable {
		public void run() {
			while (true) {
				// wait until connection is established
				_logger.info("Waiting for tcp connection to "
						+ _replica.getId());
				connect();
				_logger.info("Tcp connected " + _replica.getId());

				while (true) {
					if (Thread.interrupted()) {
						_logger.log(Level.SEVERE, "Unexpected interruption");
						close();
						return;
					}

					Message message;
					try {
						message = MessageFactory.create(_input);
					} catch (IllegalArgumentException e) {
						// end of stream or problem with socket occurred so
						// close
						// connection and try to establish it again
						_logger.log(Level.SEVERE, "Error deserializing msg", e);
						close();
						break;
					}
					if (_logger.isLoggable(Level.FINE)) {
						_logger.fine("Tcp message received ["
								+ _replica.getId() + "] " + message + " size: " + message.byteSize());
						// + " ts:"
						// + (System.currentTimeMillis() -
						// message.getSentTime()));
					}
					_network.fireReceiveMessage(message, _replica.getId());
				}
			}
		}
	}

	/**
	 * Sends specified binary packet using underlying TCP connection.
	 * 
	 * @param message
	 *            - binary packet to send
	 * @return true if sending message was successful
	 */
	public boolean send(byte[] message) {
		try {
			sendQueue.put(message);
		} catch (InterruptedException e) {
			_logger.log(Level.SEVERE, "Unexpected interruption", e);
			System.exit(1);
		}
		return true;
	}

	/**
	 * Registers new socket to this TCP connection. Specified socket should be
	 * initialized connection with other replica. First method tries to close
	 * old connection and then set-up new one.
	 * 
	 * @param socket
	 *            - active socket connection
	 * @param input
	 *            - input stream from this socket
	 * @param output
	 *            - output stream from this socket
	 */
	public synchronized void setConnection(Socket socket,
			DataInputStream input, DataOutputStream output) {
		assert socket.isConnected() : "Invalid socket state";

		// first close old connection
		close();

		// initialize new connection
		_socket = socket;
		_input = input;
		_output = output;
		_connected = true;

		// if main thread wait for this connection notify it
		notifyAll();
	}

	/**
	 * Establishes connection to host specified by this object. If this is
	 * active connection then it will try to connect to other side. Otherwise we
	 * will wait until connection will be set-up using
	 * <code>setConnection</code> method. This method will return only if the
	 * connection is established and initialized properly.
	 */
	private synchronized void connect() {
		if (_active) {
			// this is active connection so we try to connect to host
			while (true) {
				try {
					_socket = new Socket();
					// int rcvSize = _socket.getReceiveBufferSize();
					// int sendSize = _socket.getSendBufferSize();
					// _logger.info("RcvSize: " + rcvSize + ", SendSize: " +
					// sendSize);
					_socket.setReceiveBufferSize(256 * 1024);
					_socket.setSendBufferSize(256 * 1024);
					int rcvSize = _socket.getReceiveBufferSize();
					int sendSize = _socket.getSendBufferSize();
					_logger.info("RcvSize: " + rcvSize + ", SendSize: "
							+ sendSize);
					_socket.setTcpNoDelay(true);

					try {
						_socket.connect(new InetSocketAddress(_replica
								.getHostname(), _replica.getReplicaPort()));
					} catch (ConnectException e) {
						_logger.info("TCP connection with replica "
								+ _replica.getId() + " failed");

						try {
							Thread.sleep(Config.TCP_RECONNECT_TIMEOUT);
						} catch (InterruptedException e1) {
							throw new RuntimeException(e1);
						}

						continue;
					}

					_input = new DataInputStream(_socket.getInputStream());
					_output = new DataOutputStream(_socket.getOutputStream());
					_output.writeInt(_localId);
					_output.flush();
					// connection established
					break;
					// } catch (ConnectException e) {
					// // some error while establishing connection (timeout or
					// // connection refused); we have to try again and again
					// until
					// // we get the connection (we can ignore error and wait
					// // again.
					// _logger.log(Level.WARNING, "Error connecting to " +
					// _replica, e);
				} catch (IOException e) {
					// some other problem (possibly other side closes
					// connection while initializing connection); for debug
					// purpose we print this message
					_logger.log(Level.WARNING, "Error connecting to "
							+ _replica, e);
				}
				try {
					Thread.sleep(Config.TCP_RECONNECT_TIMEOUT);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			_connected = true;
			// Wake up the sender thread
			notifyAll();

		} else {
			// this is passive connection so we are waiting until other replica
			// connect to us; we will be notified by setConnection method
			while (!_connected) {
				try {
					wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		}
		assert _connected == true : "Finishing connect method in unconnected state";
	}

	/**
	 * Closes the connection clean.
	 */
	private synchronized void close() {
		try {
			_logger.info("Socket closing");
			if (_socket != null) {
				_socket.shutdownOutput();

				// TODO not clean socket closing; we have to wait until all data
				// will be received from server; after closing output stream we
				// should wait until we read all data from input stream;
				// otherwise RST will be send

				_socket.close();
				_socket = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		_connected = false;
	}

	private final static Logger _logger = Logger.getLogger(TcpConnection.class
			.getCanonicalName());
}

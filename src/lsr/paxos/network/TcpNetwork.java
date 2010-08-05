package lsr.paxos.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.KillOnExceptionHandler;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Message;

public class TcpNetwork extends AbstractNetwork implements Network, Runnable {
	private final TcpConnection[] _connections;
	private final ProcessDescriptor _p;
	private final ServerSocket _server;
	private final Thread _thread;
		
	/**
	 * Creates new network for handling connections with other replicas.
	 * 
	 * @param localId
	 *            - id of this replica
	 * @param processes
	 *            - list of other replicas
	 * @throws IOException
	 *             - if opening server socket fails
	 */
	public TcpNetwork(ProcessDescriptor p) throws IOException {
		this._p = p;
		_connections = new TcpConnection[_p.config.getN()];		
		for (int i = 0; i < _connections.length; i++) {
			if (i < p.localID) {
				_connections[i] = new TcpConnection(this, _p.config.getProcess(i));
				_connections[i].start();
			}
			if (i > p.localID) {
				_connections[i] = new TcpConnection(this, _p.config.getProcess(i), p.localID);
				_connections[i].start();
			}
		}
		_logger.fine("Opening port: " + _p.getLocalProcess().getReplicaPort());
//		_server = new ServerSocket(_p.getLocalProcess().getReplicaPort());
		_server = new ServerSocket();
		_server.setReceiveBufferSize(256*1024);
		_server.bind(new InetSocketAddress((InetAddress)null, _p.getLocalProcess().getReplicaPort()));
		
		
		_thread = new Thread(this, "TcpNetwork");		
		_thread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
		_thread.start();
	}

	/**
	 * Sends binary data to specified destination.
	 * 
	 * @param message
	 *            - binary data to send
	 * @param destination
	 *            - id of replica to send data to
	 * @return true if message was sent; false if some error occurred
	 */
	boolean send(byte[] message, int destination) {
		assert destination != _p.localID;
		return _connections[destination].send(message);
	}

	/**
	 * Main loop which accepts incoming connections.
	 */
	public void run() {
		_logger.info("TcpNetwork started");
		while (true) {
			try {
				Socket socket = _server.accept();				
				socket.setSendBufferSize(256*1024);
				initializeConnection(socket);
			} catch (IOException e) {
				// TODO: probably to many open files exception occurred;
				// should we open server socket again or just wait ant ignore
				// this exception?
				throw new RuntimeException(e);
			}
		}
	}

	private void initializeConnection(final Socket socket) {
		new Thread() {
			public void run() {
				try {
					socket.setTcpNoDelay(true);
					DataInputStream input = new DataInputStream(socket.getInputStream());
					DataOutputStream output = new DataOutputStream(socket.getOutputStream());
					int replicaId = input.readInt();

					if (replicaId < 0 || replicaId >= _p.config.getN()) {
						_logger.warning("Tcp connection with incorrect replica id. Received: " + replicaId);
						close();
						return;
					}

					_connections[replicaId].setConnection(socket, input, output);
				} catch (IOException e) {
					_logger.log(Level.WARNING, "Initialization of accepted connection failed.", e);
					close();
				}
			}

			private void close() {
				try {
					socket.shutdownOutput();
					socket.close();
				} catch (IOException e1) {
					// ignore
				}
			}
		}.start();
	}

	public void sendMessage(Message message, BitSet destinations) {
		byte[] bytes = message.toByteArray();
		_logger.fine("s0");
		// do not send message to us (just fire event)
		if (destinations.get(_p.localID))
			fireReceiveMessage(message, _p.localID);
		for (int i = destinations.nextSetBit(0); i >= 0; i = destinations.nextSetBit(i + 1)) {
			if (i != _p.localID)
				send(bytes, i);
		}
//		_logger.info("s2");
		// Not really sent, only queued for sending, 
		// but it's good enough for the notification
		fireSentMessage(message, destinations);
	}

	public void sendMessage(Message message, int destination) {
		BitSet all = new BitSet();
		all.set(destination);
		sendMessage(message, all);
	}

	public void sendToAll(Message message) {
		BitSet all = new BitSet(_p.config.getN());
		all.set(0, _p.config.getN());
		sendMessage(message, all);
	}
	

	private final static Logger _logger = Logger.getLogger(TcpNetwork.class.getCanonicalName());
}

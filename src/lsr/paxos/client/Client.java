package lsr.paxos.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.Config;
import lsr.common.Configuration;
import lsr.common.MovingAverage;
import lsr.common.PID;
import lsr.common.PerformanceLogger;
import lsr.common.PrimitivesByteArray;
import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.common.ClientCommand.CommandType;
import lsr.paxos.ReplicationException;

/**
 * Class represents TCP connection to replica. It should be used by clients, to
 * communicates with service on replicas. Only one request can be sent by client
 * at the same time. After receiving reply, next request can be sent.
 * 
 * <p>
 * Example of usage:
 * <p>
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws IOException {
 * 	Client client = new Client();
 * 	client.connect();
 * 	byte[] request = new byte[] { 0, 1, 2 };
 * 	byte[] reply = client.execute(request);
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 */
public class Client {
	// List of replicas, and information who's the leader
	private final List<PID> _replicas;
	private int _primary = -1;

	// Two variables for numbering requests
	private long _clientId = -1;
	private int _sequenceId = 0;

	// Connection timeout management - exponential moving average with upper
	// bound on max timeout. Timeout == TO_MULTIPLIER*average
	private static final int TO_MULTIPLIER = 10;
	private int _timeout;
	private static final int MAX_TIMEOUT = 30000;
	private MovingAverage _average = new MovingAverage(0.2, 5000);

	/**
	 * If couldn't connect so someone, how much time we wait before reconnecting
	 * to other person
	 */
	private static final long TIME_TO_RECONNECT = 1000;

	private Socket _socket;
	private DataOutputStream _output;
	private DataInputStream _input;
	
	/**
	 * Creates new connection used by client to connect to replicas.
	 * 
	 * @param replicas
	 *            - information about replica to connect to
	 */
	public Client(List<PID> replicas) {
		_replicas = replicas;		
	}

	public Client(Configuration config) throws IOException {
		this(config.getProcesses());
	}
	
	/** 
	 * Loads the configuration from the default configuration file,
	 * as defined in the class {@link Configuration}
	 * @throws IOException
	 */
	public Client() throws IOException {
		this(new Configuration());
	}
	

	/**
	 * Sends request to replica, to execute service with specified object as
	 * argument. This object should be known to replica, which generate reply.
	 * This method will block until response from replica is received.
	 * 
	 * @param obj
	 *            - argument for service
	 * @return reply from service
	 */
	public synchronized byte[] execute(byte[] bytes) 
	throws ReplicationException 
	{
		Request request = new Request(nextRequestId(), bytes);
		ClientCommand command = new ClientCommand(CommandType.REQUEST, request);

		while (true) {
			try {
				if (_logger.isLoggable(Level.FINE)) { 
					_logger.fine("Sending id " + request.getRequestId());
				}

				ByteArrayOutputStream prepare = new ByteArrayOutputStream();
				if (Config.javaSerialization) {
					(new ObjectOutputStream(prepare)).writeObject(command);
					_output.writeInt(prepare.size());
				} else
					command.writeToOutputStream(new DataOutputStream(prepare));

				_output.write(prepare.toByteArray());

				// Blocks only for Socket.SO_TIMEOUT
				perfLogger.log("Sending id " + request.getRequestId());
				long start = System.currentTimeMillis();

				ClientReply clientReply;
				if (Config.javaSerialization)
					try {
						clientReply = (ClientReply) ((new ObjectInputStream(_input)).readObject());
					} catch (ClassNotFoundException e) {
						throw new RuntimeException(e);
					}
					else
						clientReply = new ClientReply(_input);

				long time = System.currentTimeMillis() - start;

				switch (clientReply.getResult()) {
				case OK:
					Reply reply = new Reply(clientReply.getValue());
					_logger.fine("Reply OK");
					perfLogger.log("Reply OK");

//					if (_logger.isLoggable(Level.FINE)) {
//						_logger.fine("Reply: OK - " + reply.getRequestId() + " - Processing time: " + time);
//					}
					_average.add(time);
					return reply.getValue();

				case REDIRECT:
					int currentPrimary = PrimitivesByteArray.toInt(clientReply.getValue());						
					if (currentPrimary < 0 || currentPrimary >= _replicas.size()) {
						// Invalid ID. Ignore redirect and try next replica.
						_logger.warning("Reply: Invalid redirect received: " + currentPrimary 
								+ ". Proceeding with next replica.");
						currentPrimary = (_primary+1) % _replicas.size();
					} else {					
						perfLogger.log("Reply REDIRECT to " + currentPrimary);
						_logger.info("Reply REDIRECT to " + currentPrimary);
					}
					waitForReconnect();
					reconnect(currentPrimary);
					break;

				case NACK:
					throw new ReplicationException("Nack received: " + new String(clientReply.getValue()));

				case BUSY:
//					_logger.warning("System busy." + clientReply + " - Processing time: " + time);
					perfLogger.log("Reply BUSY");
					throw new ReplicationException(new String(clientReply.getValue()));

				default:
					throw new RuntimeException("Unknown reply type");
				}

			} catch (SocketTimeoutException e) {
				_logger.warning("Timeout waiting for answer: " + e.getMessage());
				perfLogger.log("Reply TIMEOUT");
				cleanClose();
				increaseTimeout();
				connect();
			} catch (IOException e) {
				_logger.warning("Error reading socket: " + e.getMessage());
				connect();
			}
		}
	}

	/**
	 * Tries to connect to a replica, cycling through the replicas until a
	 * connection is successfully established. After successful connection, new
	 * client id is granted which will be used for sending all messages.
	 */
	public void connect() {
		reconnect((_primary + 1) % _replicas.size());
	}

	private RequestId nextRequestId() {
		return new RequestId(_clientId, ++_sequenceId);
	}

	private void increaseTimeout() {
		_average.add(Math.min(_timeout * TO_MULTIPLIER, MAX_TIMEOUT));
		//		_logger.warning("Increasing timeout: " + _timeout);
	}

	/**
	 * Tries to reconnect to a replica, cycling through the replicas until a
	 * connection is successfully established.
	 * 
	 * @param replicaId
	 *            try to connect to this replica
	 */
	private void reconnect(int replicaId) {		
		int nextNode = replicaId;
		while (true) {
			try {
				connectTo(nextNode);
				// Success
				_primary = nextNode;
				return;			
			} catch (IOException e) {
				cleanClose();
				_logger.warning("Connect to " + nextNode + " failed: " + e.getMessage());
				//				increaseTimeout();
				nextNode = (nextNode + 1) % _replicas.size();
				waitForReconnect();
			}
		}
	}
	
	private void waitForReconnect() {
		try {
			_logger.warning("Reconnecting in " + TIME_TO_RECONNECT + "ms.");
			Thread.sleep(TIME_TO_RECONNECT);
		} catch (InterruptedException e) {
			_logger.warning("Interrupted while sleeping: " + e.getMessage());
			// Set the interrupt flag again, it will result in an  
			// InterruptException being thrown again the next time this thread
			// tries to block.
			Thread.currentThread().interrupt();
		}
	}

	private void cleanClose() {
		try {
			if (_socket != null) {
				_socket.shutdownOutput();
				_socket.close();
				_socket = null;
				_logger.info("Closing socket");
			}
		} catch (IOException e) {
			e.printStackTrace();
			_logger.log(Level.WARNING, "Not clean socket closing.");
		}
	}

	private void connectTo(int replicaId) throws IOException {
		// close previous connection if any
		cleanClose();

		PID replica = _replicas.get(replicaId);
		_logger.info("Connecting to " + replica);
		_socket = new Socket(replica.getHostname(), replica.getClientPort());

		_timeout = (int) _average.get() * TO_MULTIPLIER;
		_socket.setSoTimeout(_timeout);
		_socket.setReuseAddress(true);

		_socket.setTcpNoDelay(true);
		_output = new DataOutputStream(_socket.getOutputStream());
		_input = new DataInputStream(_socket.getInputStream());

		initConnection();

		_logger.info("Connected [p" + replicaId + "]. Timeout: " + _socket.getSoTimeout());
	}

	private void initConnection() throws IOException  {
		if (_clientId == -1) {
			_output.write('T'); // True
			_output.flush();
			_logger.fine("Waiting for id...");
			_clientId = _input.readLong();
			perfLogger = PerformanceLogger.getLogger("c"+_clientId);
			_logger.fine("New client id: " + _clientId);
		} else {
			_output.write('F'); // False
			_output.writeLong(_clientId);
			_output.flush();
		}
	}

	private PerformanceLogger perfLogger;
	private final static Logger _logger = Logger.getLogger(Client.class.getCanonicalName());
}

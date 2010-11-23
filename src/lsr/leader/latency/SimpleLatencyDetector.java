/**
 * 
 */
package lsr.leader.latency;

import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Handler;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.leader.messages.Ping;
import lsr.leader.messages.Pong;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandlerAdapter;
import lsr.paxos.network.Network;

/**
 * Simple implementation of a latency detector. The LD periodically sends ping
 * pong messages to all the processes and compute the RTT. The variable
 * NB_DROP_ALLOWED correspond to the number of consecutive non-response allowed
 * before the LD report an infinite RTT value.
 * 
 * @author Benjamin Donz?
 * @author Nuno Santos
 */
public class SimpleLatencyDetector implements LatencyDetector {
	/** Upper bound on transmission time of a message */
	public final static String DELTA = "leader.delta";
	private final int delta;
	private final static int DEFAULT_DELTA = 1000;

	/**
	 * How long the latency detector waits until sending ping message. In
	 * milliseconds
	 */
	public final static String SEND_PERIOD = "leader.latdet.sendPeriod";
	private final int sendPeriod;
	private final static int DEFAULT_SEND_PERIOD = 5000;

	/** Number of consecutive loss before a process is suspected to crash */
	private final static int NB_DROP_ALLOWED = 1;

	private final SingleThreadDispatcher executor;
	private final Network network;

	/*
	 * Adding and removing elements from the listener arrays may be performed by
	 * external threads, while iterating is done by the dispatcher thread.
	 * Therefore, access to the array must be made thread-safe
	 */
	private CopyOnWriteArrayList<LatencyDetectorListener> listeners;

	/** precision in milliseconds */
	private double[] rttVector;
	private final int n;

	private long pingTime;
	private boolean[] receivedPong;
	private int[] packetDropped;
	private int seqNum = 0;

	/** Receives notifications of messages from the network class */
	private InnerMessageHandler innerHandler;

	/** Sends pings */
	private ScheduledFuture<SendPingTask> sendPingTask;
	private ScheduledFuture<NotifyListenersTask> notifyTask;

	public SimpleLatencyDetector(ProcessDescriptor p, Network network,
			SingleThreadDispatcher executor) {
		this.sendPeriod = p.config.getIntProperty(SEND_PERIOD,
				DEFAULT_SEND_PERIOD);
		this.delta = p.config.getIntProperty(DELTA, DEFAULT_DELTA);

		if (sendPeriod <= 2 * delta) {
			throw new IllegalArgumentException("Send period (" + sendPeriod
					+ ") must be bigger than 2*delta: " + (2 * delta));
		}

		this.executor = executor;
		this.network = network;
		this.n = p.config.getN();

		_logger.info("Configuration: PING PERIOD=" + sendPeriod);

		rttVector = new double[n];
		receivedPong = new boolean[n];
		packetDropped = new int[n];
		listeners = new CopyOnWriteArrayList<LatencyDetectorListener>();
		innerHandler = new InnerMessageHandler();
	}

	@SuppressWarnings("unchecked")
	void onStart() {
		_logger.info("SimpleLatencyDetector: starting");
		executor.checkInDispatcher();

		for (int i = 0; i < n; i++) {
			receivedPong[i] = false;
			packetDropped[i] = 0;
			rttVector[i] = Double.MAX_VALUE;
		}

		Network.addMessageListener(MessageType.Ping, innerHandler);
		Network.addMessageListener(MessageType.Pong, innerHandler);
		// checkIsInExecutorThread();
		if (sendPingTask != null) {
			sendPingTask.cancel(true);
			_logger.warning("Latency Detector already running");
		}
		// Repeat execution
		sendPingTask = (ScheduledFuture<SendPingTask>) executor
				.scheduleAtFixedRate(new SendPingTask(), 0, sendPeriod,
						TimeUnit.MILLISECONDS);
	}

	void onStop() {
		executor.checkInDispatcher();
		_logger.info("SimpleLatencyDetector: stopping");
		Network.removeMessageListener(MessageType.Ping, innerHandler);
		Network.removeMessageListener(MessageType.Pong, innerHandler);

		if (sendPingTask != null) {
			sendPingTask.cancel(true);
			sendPingTask = null;
		}
	}

	public double[] getRTTVector() {
		return Arrays.copyOf(rttVector, rttVector.length);
	}

	public void registerLatencyDetectorListener(LatencyDetectorListener listener) {
		listeners.addIfAbsent(listener);
	}

	public void removeLatencyDetectorListener(LatencyDetectorListener listener) {
		listeners.remove(listener);
	}

	final class SendPingTask extends Handler {
		@SuppressWarnings("unchecked")
		public void handle() {
			executor.checkInDispatcher();
			assert notifyTask == null : "Starting a new PING rounds before finishing the previous one";
			_logger.fine("Sending pings.");

			Arrays.fill(receivedPong, false);

			seqNum++;
			Ping pingMsg = new Ping(seqNum);
			pingTime = System.nanoTime();
			network.sendToAll(pingMsg);
			// Notify the listeners 2.Delta after sending PINGs,
			// that's the upper estimate of how long it takes to receive all
			// messages
			// Otherwise, there's a big delay between measurement and
			// notification.
			notifyTask = (ScheduledFuture<NotifyListenersTask>) executor
					.schedule(new NotifyListenersTask(), 2 * delta,
							TimeUnit.MILLISECONDS);
		}
	}

	final class NotifyListenersTask extends Handler {
		@Override
		public void handle() {
			// Optimization
			for (int i = 0; i < n; i++) {
				if (!receivedPong[i]) {
					packetDropped[i]++;
					if (packetDropped[i] > NB_DROP_ALLOWED) {
						rttVector[i] = Double.MAX_VALUE;
					}
				}
			}

			if (_logger.isLoggable(Level.INFO)) {
				double[] aux = Arrays.copyOf(rttVector, rttVector.length);
				Arrays.sort(aux);
				_logger.info("New RTT vector: " + Arrays.toString(rttVector)
						+ ", Maj: " + aux[n / 2]);
			}

			// BroadcastUpdateTask
			for (LatencyDetectorListener listener : listeners) {
				listener.onNewRTTVector(Arrays.copyOf(rttVector,
						rttVector.length));
			}
			notifyTask = null;
		}

	}

	final class InnerMessageHandler extends MessageHandlerAdapter {
		@Override
		public void onMessageReceived(final Message msg, final int sender) {
			// Execute on the dispatcher thread.
			executor.execute(new Handler() {
				@Override
				public void handle() {
					switch (msg.getType()) {
					case Ping:
						// _logger.fine("Received " + msg " + sender);
						Pong pongMsg = new Pong(msg.getView());
						network.sendMessage(pongMsg, sender);
						break;

					case Pong:
						if (msg.getView() == seqNum) {
							// Convert from nano seconds to milliseconds using
							// doubles
							rttVector[sender] = (System.nanoTime() - pingTime) / 1000000.0;
							receivedPong[sender] = true;
							packetDropped[sender] = 0;
						}
						break;

					default:
						_logger.severe("Wrong message type received!!!");
						System.exit(1);
					}
				}
			});
		}
	}

	public void start() throws Exception {
		executor.executeAndWait(new Handler() {
			@Override
			public void handle() {
				onStart();
			}
		});
	}

	public void stop() throws Exception {
		executor.executeAndWait(new Handler() {
			@Override
			public void handle() {
				onStop();
			}
		});
	}

	public int getDelta() {
		return delta;
	}

	public int getSendPeriod() {
		return sendPeriod;
	}

	private final static Logger _logger = Logger
			.getLogger(SimpleLatencyDetector.class.getCanonicalName());
}

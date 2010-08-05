package lsr.paxos.replica;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.PrimitivesByteArray;
import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.common.ClientReply.Result;
import lsr.paxos.NotLeaderException;
import lsr.paxos.Paxos;

/**
 * This class handles all commands from the clients.
 * 
 */
public class ReplicaCommandCallback implements CommandCallback {
	private final Paxos _paxos;

	/**
	 * Requests received but waiting ordering. Map between the request id to the
	 * client proxy that is waiting for its ordering
	 */
	private final Map<RequestId, ClientProxy> _pendingRequests;

	/**
	 * Keeps the last reply for each client. Necessary for retransmissions.
	 */
	private final Map<Long, Reply> _lastReplies;

	public ReplicaCommandCallback(Paxos paxos) {
		_paxos = paxos;
		_lastReplies = new Hashtable<Long, Reply>();
		_pendingRequests = new Hashtable<RequestId, ClientProxy>();
	}

	/**
	 * Executes command received from specified client.
	 * 
	 * @param command
	 *            - received client command
	 * @param client
	 *            - client which request this command
	 * @see ClientCommand, ClientProxy
	 */
	public void execute(ClientCommand command, ClientProxy client) {
		try {
			switch (command.getCommandType()) {
			case REQUEST:
				if (!Replica.BENCHMARK) {
					if (_logger.isLoggable(Level.INFO)) {
						_logger
								.info("Received request "
										+ command.getRequest());// + " from " +
						// client);
					}
				}
				Request request = command.getRequest();

				// Nuno: Load shedding
				if (_paxos.getDispatcher().isBusy()) {
					_logger.warning("Busy. Request refused "
							+ request.getRequestId());
					client
							.send(new ClientReply(Result.BUSY, "Busy"
									.getBytes()));
					break;
				}

				if (isNewRequest(request))
					handleNewRequest(client, request);
				else
					handleOldRequest(client, request);

				break;

			default:
				_logger.warning("Received invalid command " + command
						+ " from " + client);
				client.send(new ClientReply(Result.NACK, "Unknown command."
						.getBytes()));
				break;
			}
		} catch (IOException e) {
			_logger.warning("Cannot execute command: " + e.getMessage());
		}
	}

	/**
	 * Caches the reply from the client. If the connection with the client is
	 * still active, then reply is sent.
	 * 
	 * @param request
	 *            - request for which reply is generated
	 * @param reply
	 *            - reply to send to client
	 */
	public void handleReply(Request request, Reply reply) {
		cacheReply(request, reply);

		ClientProxy cProxy = _pendingRequests.remove(reply.getRequestId());
		if (cProxy == null)
			return;

		try {
			cProxy.send(new ClientReply(Result.OK, reply.toByteArray()));
		} catch (IOException e) {
			// cannot send message to the client; we can ignore this because
			// user should send request again
		}
	}

	private void cacheReply(Request request, Reply reply) {
		_lastReplies.put(request.getRequestId().getClientId(), reply);
	}

	private void handleNewRequest(ClientProxy client, Request request)
			throws IOException {
		if (!_paxos.isLeader()) {
			int redirectID;
			if (_paxos.getLeaderId() != -1) {
				_logger.info("Redirecting client to leader: "
						+ _paxos.getLeaderId());
				redirectID = _paxos.getLeaderId();
			} else {
				_logger
						.warning("Leader undefined! Sending null redirect (-1).");
				redirectID = -1;
			}
			client.send(new ClientReply(Result.REDIRECT, PrimitivesByteArray
					.fromInt(redirectID)));
			return;
		}

		try {
			_paxos.propose(request);
			// store for later retrieval by the replica thread (this client
			// proxy will be notified when this request will
			// be executed)
			_pendingRequests.put(request.getRequestId(), client);
		} catch (NotLeaderException e) {
			// Should never fail, since we checked previously for leadership
			throw new AssertionError("Unexpected exception: " + e.getMessage());
		}
	}

	private void handleOldRequest(ClientProxy client, Request request)
			throws IOException {
		Reply lastReply = getLastReply(request);

		// resent the reply if known
		if (lastReply.getRequestId().equals(request.getRequestId()))
			client.send(new ClientReply(Result.OK, lastReply.toByteArray()));
	}

	private Reply getLastReply(Request request) {
		return _lastReplies.get(request.getRequestId().getClientId());
	}

	/**
	 * Checks whether we reply for the request with greater or equal request id.
	 * 
	 * @param request
	 *            - request from client
	 * @return <code>true</code> if we reply to request with greater or equal id
	 * @see Request
	 */
	private boolean isNewRequest(Request request) {
		Reply lastReply = getLastReply(request);
		return lastReply == null
				|| request.getRequestId().getSeqNumber() > lastReply
						.getRequestId().getSeqNumber();
	}

	private static final Logger _logger = Logger
			.getLogger(ReplicaCommandCallback.class.getCanonicalName());
}

package lsr.paxos;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Config;
import lsr.common.Dispatcher;
import lsr.common.Dispatcher.Priority;
import lsr.common.Dispatcher.PriorityTask;
import lsr.common.Pair;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.CatchUpQuery;
import lsr.paxos.messages.CatchUpResponse;
import lsr.paxos.messages.CatchUpSnapshot;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Storage;

public class CatchUp {

	private Storage _storage;
	private SnapshotProvider _snapshotProvider;
	private Network _network;
	private Paxos _paxos;

	private Dispatcher _dispatcher;

	/**
	 * Current CatchUp run mode - either requesting snapshot, or requesting
	 * instances
	 */
	private Mode mode = Mode.Normal;

	private enum Mode {
		Normal, Snapshot
	};

	/* Initial, conservative value. Updated as a moving average. */
	private long _resendTimeout = Config.RETRANSMIT_TIMEOUT;

	/** moving average factor used for changing timeout */
	private final double _convergenceFactor = 0.2;

	private PriorityTask _checkCatchUpTask = null;
	private PriorityTask _doCatchupTask = null;

	/**
	 * Replica rating rules for catch-up:
	 * <ol>
	 * <li>each replica starts with rating 0
	 * <li>as we send the request, we decrease the value by number of instances
	 * requested
	 * <li>as we receive the request, we increase the value by number of
	 * instances acquired
	 * <li>if the sendMessage function throws an exception, the rating is set to
	 * negative value
	 * <li>An empty, not periodic response reaches us, we decrees down by 5 the
	 * value (but not under 0)
	 * </ol>
	 * 
	 * Selecting best replica:
	 * <ol>
	 * <li>We check, if we were not asked to select the leader (happens if other
	 * replica answered it has no value for us)
	 * <li>We select replicas that is neither me not leader
	 * <li>the replica with best rating is chosen
	 * <li>if the best rating is negative, we're catching up with the leader,
	 * and we reset all ratings; otherwise we catch up with the selected one
	 * </ol>
	 */

	/** holds replica rating for choosing best replica for catch-up */
	int[] replicaRating;

	public CatchUp(SnapshotProvider snapshotProvider, Paxos paxos, Storage storage, Network network) {
		_snapshotProvider = snapshotProvider;
		_network = network;
		_dispatcher = paxos.getDispatcher();
		MessageHandler handler = new InnerMessageHandler();
		_network.addMessageListener(MessageType.CatchUpQuery, handler);
		_network.addMessageListener(MessageType.CatchUpResponse, handler);
		_network.addMessageListener(MessageType.CatchUpSnapshot, handler);
		_paxos = paxos;
		_storage = storage;
		replicaRating = new int[_storage.getN()];
	}

	public void start() {
		// TODO: verify catchup is correct. It's being triggered too often when latency is high. 
		// Workaround: disable catchup for the benchmarks
		if (ProcessDescriptor.getInstance().benchmarkRun) {
			return;
		}
		
		scheduleCheckCatchUpTask();
	}

	/** Called to initiate catchup. */
	public void startCatchup() {
		scheduleCatchUpTask(Priority.Low, 0);
	}

	public void forceCatchup() {
		scheduleCatchUpTask(Priority.Normal, 0);
	}

	private void scheduleCheckCatchUpTask() {
		if (_checkCatchUpTask == null) {
			logger.info("scheduleCheckCatchUpTask()");
			if (_doCatchupTask != null) {
				_doCatchupTask.cancel();
				_doCatchupTask = null;
			}

			_checkCatchUpTask = _dispatcher.scheduleAtFixedRate(new CheckCatchupTask(), Priority.Normal,
					Config.PERIODIC_CATCHUP_TIMEOUT, Config.PERIODIC_CATCHUP_TIMEOUT);
		} else {
			assert !_checkCatchUpTask.isCanceled();
		}
	}

	private void scheduleCatchUpTask(Priority priority, long delay) {
		if (_checkCatchUpTask != null) {
			// While trying to do catchup, do not check if catchup is needed
			_checkCatchUpTask.cancel();
			_checkCatchUpTask = null;
		}

		if (_doCatchupTask != null) {
			// Already doing catchup. Do not reschedule if the
			// new request is of lower or equal priority. (higher numeric value)
			if (priority.compareTo(_doCatchupTask.priority) >= 0) {
				return;
			}

			_doCatchupTask.cancel();
			_doCatchupTask = null;
		}

		// Use low priority, so that processing incoming messages
		// take precedence over catchup
		logger.info("Activating catchup. Priority: " + priority);
		_doCatchupTask = _dispatcher.scheduleWithFixedDelay(new DoCatchUpTask(), priority, delay, _resendTimeout);
	}

	// /**
	// * Called when catchup is already enabled but we want
	// * to execute again the main handler
	// * @param priority
	// */
	// private void rescheduleCatchupTask(Priority priority) {
	// assert _checkCatchUpTask == null;
	//
	// _doCatchupTask.cancel();
	// _doCatchupTask = _dispatcher.scheduleWithFixedDelay(
	// new DoCatchUpTask(),
	// priority,
	// _resendTimeout,
	// _resendTimeout);
	// }

	class CheckCatchupTask implements Runnable {
		@Override
		public void run() {
			logger.info("CheckCatchupTask running");
			// TODO: Consider catchup on the context of variable window size.
			// There may be several instances open.
			int wndSz = ProcessDescriptor.getInstance().windowSize;
			// Still on the window?
			if (_storage.getFirstUncommitted() + wndSz > _storage.getLog().getNextId()) {
				return;
			}
						

			// It may happen, that after view change, the leader will send to
			// himself propose for old instances
			if (_paxos.isLeader()) {
				return;
			}

			// Start catchup
			scheduleCatchUpTask(Priority.Normal, 0);
		}
	}

	/**
	 * Catch-up thread is sleeping until someone requests catch-up (either we
	 * got a message for instance out of current window, or we know from alive
	 * message that a newer is started)
	 */

	/**
	 * Main catch-up process: we're creating (and later updating) the list of
	 * undecided instances, and sending it.
	 * 
	 * We're trying to reach best replica possible, and as we get the needed
	 * information, we exit.
	 */
	class DoCatchUpTask implements Runnable {
		@SuppressWarnings("unchecked")
		public void run() {
			logger.info("DoCatchupTask running");
			int target;
			int requestedInstanceCount = 0;

			// If in normal mode, we're sending normal request;
			// if in snapshot mode, we request the snapshot
			// TODO: send values after snapshot automatically
			CatchUpQuery query = new CatchUpQuery(_storage.getStableStorage().getView(), new int[0], new Pair[0]);
			if (mode == Mode.Snapshot) {
				query.setSnapshotRequest(true);
			} else if (mode == Mode.Normal) {
				requestedInstanceCount = fillUnknownList(query);
				if (_storage.getFirstUncommitted() == _storage.getStableStorage().getLog().getNextId())
					query.setPeriodicQuery(true);
			} else
				assert false : "Wrong state of the catch up";

			target = getBestCatchUpReplica();
			if(_storage.getLocalId()==_paxos.getLeaderId())
			{
				logger.warning("Leader triggered itself for catch-up!");
				return;
			}
			assert target != _storage.getLocalId() : "Selected self for catch-up";

			_network.sendMessage(query, target);

			// Modifying the rating of replica we're catching up with
			// We don't count the additional logSize+1 number requested

			replicaRating[target] -= requestedInstanceCount;

			logger.info("Sent " + query.toString() + " to [p" + target + "]");
		}
	}

	/**
	 * the predicate being true when the catch-up mechanism should finish
	 */
	private boolean assumeSucceded() {
		// the current is OK for catch-up caused by the window violation, and
		// not for periodical one. Consider writing a method giving more sense
		return _storage.isInWindow(_storage.getLog().getNextId() - 1);
	}

	/**
	 * If a replica responds that it has even not a single value for us, we're
	 * trying to catch-up with the leader
	 */
	private boolean askLeader = false;

	/**
	 * Should return the ID to replica best suiting for catch-up; may change
	 * during catching-up
	 */
	private int getBestCatchUpReplica() {
		// TODO: verify code changing replica ratings

		if (askLeader) {
			askLeader = false;
			return _paxos.getLeaderId();
		}

		// BitSet candidates has all processes without his and the leader
		BitSet candidates = new BitSet(_storage.getN());
		candidates.set(0, _storage.getN());
		candidates.clear(_storage.getLocalId());
		candidates.clear(_paxos.getLeaderId());

		// replica with greatest rating is used
		int i = candidates.nextSetBit(0);
		int bestReplica = i;

		for (; i >= 0; i = candidates.nextSetBit(i + 1)) {
			if (replicaRating[i] > replicaRating[bestReplica])
				bestReplica = i;
		}

		// If a replica has negative rating, we catch-up with the leader
		if (replicaRating[bestReplica] < 0) {
			bestReplica = _paxos.getLeaderId();

			// For all but leader the value is set to zero
			for (int j = 0; j < replicaRating.length; ++j) {
				if (j != bestReplica)
					replicaRating[j] = 0;
			}

		}

		return bestReplica;
	}

	/**
	 * Generates (ascending) list of instance numbers, which we consider for
	 * undecided yet on basis of the log, and adds the next instance number on
	 * the end of this list
	 * 
	 * @param query
	 * @return count of instances embedded
	 */
	private int fillUnknownList(CatchUpQuery query) {
		List<Integer> unknownList = new Vector<Integer>();
		List<Pair<Integer, Integer>> unknownRange = new Vector<Pair<Integer, Integer>>();

		SortedMap<Integer, ConsensusInstance> log = _storage.getLog().getInstanceMap();

		if (log.isEmpty())
			return 0;

		int begin = -1;
		boolean previous = false;
		int lastKey = log.lastKey();
		int count = 1;

		ConsensusInstance instance;
		for (int i = Math.max(_storage.getFirstUncommitted(), log.firstKey()); i <= lastKey; ++i) {
			instance = log.get(i);

			if (instance == null)
				continue;

			if (instance.getState() != LogEntryState.DECIDED) {
				count++;
				if (!previous) {
					begin = i;
					previous = true;
				}
			} else if (previous) {
				assert begin != -1 : "Problem in unknown list creation 1";
				if (begin == i - 1)
					unknownList.add(begin);
				else
					unknownRange.add(new Pair<Integer, Integer>(begin, i - 1));
				previous = false;
			}
		}

		if (previous) {
			assert begin != -1 : "Problem in unknown list creation 2";
			if (begin == lastKey)
				unknownList.add(begin);
			else
				unknownRange.add(new Pair<Integer, Integer>(begin, lastKey));
		}

		unknownList.add(lastKey + 1);

		query.setInstanceIdList(unknownList);
		query.setInstanceIdRangeList(unknownRange);

		return count;
	}

	private void handleSnapshot(CatchUpSnapshot msg, int sender) {
		mode = Mode.Normal;
		Snapshot snapshot = msg.getSnapshot();

		logger.info("Catch-up snapshot from [p" + sender + "] : " + msg.toString());

		replicaRating[sender] = Math.max(replicaRating[sender], 5);

		_snapshotProvider.handleSnapshot(snapshot, new Object());

	}

	/**
	 * Procedure handling the catch-up response - if it's empty, it's dropped,
	 * otherwise we're adding proper event for the dispatcher
	 */
	private void handleResponse(CatchUpResponse response, int sender) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Catch-up from [p" + sender + "] : " + response.toString());
		}

		if (response.isSnapshotOnly()) {
			// As for now, we're requesting the snapshot; we could also ask
			// other replicas if they don't have the older instances (if we're
			// missing few only) or if they have a newer snapshot
			mode = Mode.Snapshot;

			int maxRated = 0;
			for (int i = 0; i < replicaRating.length; ++i) {
				if (replicaRating[i] > replicaRating[maxRated])
					maxRated = i;
			}

			if (maxRated != sender)
				replicaRating[sender] = replicaRating[maxRated] + 1;

			logger.info("Catch-up from [p" + sender + "] : " + response.toString());

			scheduleCatchUpTask(Priority.Normal, _resendTimeout);
			return;
		}

		List<ConsensusInstance> logFragment = response.getDecided();

		if (logFragment.isEmpty()) {
			if (response.isPeriodicQuery()) {
				scheduleCatchUpTask(Priority.Normal, _resendTimeout);
				return;
			}

			// We decrees the rating of a replica, who has no value for us
			// at all
			replicaRating[sender] = Math.max(0, replicaRating[sender] - 5);
			askLeader = true;

			scheduleCatchUpTask(Priority.Normal, _resendTimeout);
			return;
		}

		replicaRating[sender] += 2 * logFragment.size();

		long processingTime = System.currentTimeMillis() - response.getRequestTime();
		// As timeout base, we use double processing time
		_resendTimeout = (long) (((1 - _convergenceFactor) * _resendTimeout) + (_convergenceFactor * (3 * processingTime)));
		_resendTimeout = Math.max(Config.CATCHUP_MIN_RESEND_TIMEOUT, _resendTimeout);

		if (logger.isLoggable(Level.FINE)) {
			logger.fine("Changing resend timeout for Catch-Up to " + _resendTimeout);
		}

		handleCatchUpEvent(logFragment);

		// _paxos.getDispatcher().execute(new CatchUpEvent(logFragment,
		// response.isLastPart()));
	}

	/**
	 * Main procedure for the replica responding to query.
	 * 
	 * The replica checks if the requested instances are decided by it, and if
	 * yes - it appends them to response.
	 */
	private void handleQuery(CatchUpQuery query, int sender) {

		if (query.isSnapshotRequest()) {
			Message m;
			Snapshot lastSnapshot = _storage.getStableStorage().getLastSnapshot();

			if (lastSnapshot != null)
				m = new CatchUpSnapshot(_storage.getStableStorage().getView(), query.getSentTime(), lastSnapshot);
			else
				m = new CatchUpResponse(_storage.getStableStorage().getView(), query.getSentTime(),
						new Vector<ConsensusInstance>());

			_network.sendMessage(m, sender);

			logger.info("Got " + query.toString() + " from [p" + sender + "]");
			return;
		}

		SortedMap<Integer, ConsensusInstance> log = _storage.getLog().getInstanceMap();

		if (log.isEmpty()) {
			if (_storage.getStableStorage().getLastSnapshot() != null)
				sendSnapshotOnlyResponse(query, sender);
			return;
		}

		Integer lastKey;
		try {
			lastKey = log.lastKey();
		} catch (NoSuchElementException e) {
			assert false : "Could not fetch last key from non-empty log";
			throw e;
		}

		InnerResponseSender responseSender = new InnerResponseSender(query, sender);

		int i;
		for (Pair<Integer, Integer> range : query.getInstanceIdRangeArray()) {
			for (i = range.key(); i <= range.value() && i <= lastKey; ++i) {
				ConsensusInstance consensusInstance = log.get(i);

				if (consensusInstance == null) {
					sendSnapshotOnlyResponse(query, sender);
					return;
				}

				if (consensusInstance.getState() == LogEntryState.DECIDED)
					responseSender.add(consensusInstance);
			}
		}

		// We check for requested id's
		for (int instanceId : query.getInstanceIdArray()) {
			if (instanceId >= lastKey)
				continue;
			ConsensusInstance consensusInstance = log.get(instanceId);

			if (consensusInstance == null) {
				sendSnapshotOnlyResponse(query, sender);
				return;
			}

			if (consensusInstance.getState() == LogEntryState.DECIDED)
				responseSender.add(consensusInstance);
		}

		// If we have any newer values, we're sending them as well

		// Nuno: The replica might have learned the newer values
		// by itself. Let her send a new query if needed.

		responseSender.flush();

		logger.info("Got " + query.toString() + " from [p" + sender + "]");
	}

	private void sendSnapshotOnlyResponse(CatchUpQuery query, int sender) {
		assert _storage.getStableStorage().getLastSnapshot() != null;

		List<ConsensusInstance> list = new Vector<ConsensusInstance>();

		CatchUpResponse response = new CatchUpResponse(_storage.getStableStorage().getView(), query.getSentTime(), list);
		response.setSnapshotOnly(true);

		_network.sendMessage(response, sender);

		logger.info("Got " + query.toString() + " from [p" + sender + "] (responding: snapshot only)");
	}

	/**
	 * updates the storage and request deciding the caught-up instances
	 */
	private void handleCatchUpEvent(List<ConsensusInstance> logFragment) {

		for (int i = 0; i < logFragment.size(); ++i) {

			ConsensusInstance newInstance = logFragment.get(i);

			ConsensusInstance oldInstance = _storage.getLog().getInstance(newInstance.getId());

			// A snapshot && log truncate took place; must have been
			// decided
			if (oldInstance == null)
				continue;

			// If, in the meantime, the protocol took the decision, then
			// we're not going on (shouldn't decide twice)
			if (oldInstance.getState() == LogEntryState.DECIDED)
				continue;

			oldInstance.setValue(newInstance.getView(), newInstance.getValue());

			_paxos.decide(oldInstance.getId());
		}
	}

	/** Needed to be notified about messages for catch-up */
	private class InnerMessageHandler implements MessageHandler {
		public void onMessageReceived(final Message msg, final int sender) {
			_dispatcher.dispatch(new Runnable() {
				@Override
				public void run() {
					switch (msg.getType()) {
						case CatchUpResponse:
							handleResponse((CatchUpResponse) msg, sender);
							checkCatchupSucceded();
							break;
						case CatchUpQuery:
							handleQuery((CatchUpQuery) msg, sender);
							break;
						case CatchUpSnapshot:
							handleSnapshot((CatchUpSnapshot) msg, sender);
							checkCatchupSucceded();
							break;
						default:
							assert false : "Unexpected message type: " + msg.getType();
					}
				}
			});
		}

		public void onMessageSent(Message message, BitSet destinations) {
			// Empty
		}
	}

	void checkCatchupSucceded() {
		if (assumeSucceded()) {
			mode = Mode.Normal;
			logger.info("Catch-up succeeded");
			scheduleCheckCatchUpTask();
		}
	}

	private class InnerResponseSender {
		private final CatchUpQuery _query;
		private final int _sender;
		private long _currentSize;
		private final long _responseSize;

		/**
		 * Contains instances matching requested id's that we have marked as
		 * decided
		 */
		private final List<ConsensusInstance> _availableInstances;
		private boolean _anythingSent = false;

		public InnerResponseSender(CatchUpQuery query, int sender) {
			_query = query;
			_sender = sender;
			_availableInstances = new Vector<ConsensusInstance>();
			_responseSize = (new CatchUpResponse(0, 0, new ArrayList<ConsensusInstance>())).toByteArray().length;
			_currentSize = _responseSize;
		}

		public void add(ConsensusInstance instance) {
			long instanceSize = instance.byteSize();

			if (_currentSize + instanceSize > ProcessDescriptor.getInstance().maxUdpPacketSize) {
				// Config.MAX_UDP_PACKET_SIZE) {
				sendAvailablePart();
				_currentSize = _responseSize;
			}
			_currentSize += instanceSize;
			_availableInstances.add(instance);
		}

		public void flush() {
			if (!_availableInstances.isEmpty() || _anythingSent == false) {

				CatchUpResponse response = new CatchUpResponse(_storage.getStableStorage().getView(), _query
						.getSentTime(), _availableInstances);

				if (_query.isPeriodicQuery())
					response.setPeriodicQuery(true);

				_network.sendMessage(response, _sender);
			}
		}

		private void sendAvailablePart() {
			CatchUpResponse response = new CatchUpResponse(_storage.getStableStorage().getView(), _query.getSentTime(),
					_availableInstances);
			response.setLastPart(false);

			_network.sendMessage(response, _sender);

			_availableInstances.clear();
			_anythingSent = true;
		}
	}

	private final static Logger logger = Logger.getLogger(CatchUp.class.getCanonicalName());

}

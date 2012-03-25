package lsr.paxos.replica;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.Pair;
import lsr.common.Reply;
import lsr.common.SingleThreadDispatcher;
import lsr.service.Service;

/**
 * This class is responsible for generating correct sequence number of executed
 * request and passing it to underlying service. It also keeps track of snapshot
 * (made by service or received from paxos) and updates state of underlying
 * service and request sequence number.
 * <p>
 * It is used because batching is used in paxos protocol. One consensus instance
 * can contain more than one request from client. Because of that sequence
 * number of executed request on service is different than id of consensus
 * instance. Assume we have following instances decided:
 * 
 * <pre>
 * ConsensusInstance 0
 *   Request 0
 *   Request 1
 * ConsensusInstance 1
 *   Request 2
 *   Request 3
 *   Request 4
 * ConsensusInstance 2
 *   Request 5
 * </pre>
 * The first consensus instance contains 2 requests, second contains 3 requests
 * and the last instance contains only one request. It is important that we call
 * <code>execute()</code> method on underlying service with following arguments:
 * 
 * <pre>
 * service.execute(Request0, 0)
 * service.execute(Request1, 1)
 * service.execute(Request2, 2)
 * service.execute(Request3, 3)
 * service.execute(Request4, 4)
 * service.execute(Request5, 5)
 * </pre>
 * <p>
 * 
 * Usage example:
 * <p>
 * Execute consensus instances on service proxy:
 * 
 * <pre>
 * Service service = ...;
 * Map<Integer, List<Reply>> responsesCache = ...;
 * SingleThreadDispatcher dispatcher = ...;
 * ConsensusInstance[] instances = ...; 
 * 
 * ServiceProxy proxy = new ServiceProxy(service, responsesCatche, dispatcher);
 * for(ConsensusInstance instance : instances) {
 *   for(Request request : batcher.unpack(instance.getValue()) {
 *     byte[] result = proxy.execute(request);
 *     responsesCache.put(instance.getId(), new Reply(request.getRequestId(), result));
 *   }
 *   proxy.instanceExecuted(instance.getId());
 * }
 * </pre>
 * Update service from snapshot:
 * 
 * <pre>
 * Snapshot snapshot = ...; // from paxos protocol or from disc
 * 
 * proxy.updateToSnapshot(snapshot);
 * </pre>
 * 
 * @see Service
 */
public class ServiceProxy {

    /**
     * Sorted list of request sequence number starting each consensus instance.
     * <p>
     * Example. Assume we executed following consensus instances:
     * 
     * <pre>
     * ConsensusInstance 0
     *   Request 0
     *   Request 1
     * ConsensusInstance 1
     *   Request 2
     *   Request 3
     *   Request 4
     * ConsensusInstance 2
     *   Request 5
     * </pre>
     * 
     * Then this list will contain following pairs:
     * 
     * <pre>
     * [0, 0]
     * [1, 2]
     * [2, 5]
     * </pre>
     * 
     * The sequence number of first request in consensus instance 2 is 5, etc.
     */
    private LinkedList<Pair<Integer, Integer>> startingSeqNo =
            new LinkedList<Pair<Integer, Integer>>();

    /** The sequence number of next request passed to service. */
    private int nextSeqNo = 0;

    /** The sequence number of first request executed after last snapshot. */
    private int lastSnapshotNextSeqNo = -1;

    /**
     * Describes how many requests on should be skipped. Used only after
     * updating from snapshot.
     */
    private int skip = 0;

    /**
     * Holds responses for skipped requests. Used only after updating from
     * snapshot.
     */
    private Queue<Reply> skippedCache;

    /** Used for keeping requestId for snapshot purposes. */
    private ClientRequest currentRequest;

    private final Service service;
    private final Map<Integer, List<Reply>> responsesCache;
    private final SingleThreadDispatcher replicaDispatcher;

    /**
     * Creates new <code>ServiceProxy</code> instance.
     * 
     * @param service - the service wrapped by this proxy
     * @param responsesCache - the cache of responses from service
     * @param replicaDispatcher - the dispatcher used in replica
     */
    public ServiceProxy(Service service, Map<Integer, List<Reply>> responsesCache,
                        SingleThreadDispatcher replicaDispatcher) {
        this.service = service;
        this.replicaDispatcher = replicaDispatcher;
        this.responsesCache = responsesCache;
        startingSeqNo.add(new Pair<Integer, Integer>(0, 0));
    }

    /**
     * Executes the request on underlying service with correct sequence number.
     * 
     * @param request - the request to execute on service
     * @return the reply from service
     */
    public byte[] execute(ClientRequest request) {
        nextSeqNo++;
        if (skip > 0) {
            skip--;
            assert !skippedCache.isEmpty();
            return skippedCache.poll().getValue();
        } else {
            currentRequest = request;
            return service.execute(request.getValue(), nextSeqNo - 1);
        }
    }

    /** Update the internal state to reflect the execution of a nop request */
    public void executeNop() {
        // TODO: Update snapshotting and recovery to support no-op requests
        nextSeqNo++;
    }

    /**
     * Notifies this service proxy that all request from specified consensus
     * instance has been executed.
     * 
     * @param instanceId - the id of executed consensus instance
     */
    public void instanceExecuted(int instanceId) {
        startingSeqNo.add(new Pair<Integer, Integer>(instanceId + 1, nextSeqNo));
    }

    /**
     * Informs the service that the recovery process has been finished, i.e.
     * that the service is at least at the state later than by crashing.
     * 
     * Please notice, for some crash-recovery approaches this can mean that the
     * service is a lot further than by crash.
     */
    public void recoveryFinished() {
        service.recoveryFinished();
    }

    /**
     * Truncates the startingSeqNo list so that value of first pair on the list
     * will be less or equal than specified <code>lowestSeqNo</code> and value
     * of second pair will be greater than <code>lowestSeqNo</code>. In other
     * words, key of first pair will equal to id of consensus instance that
     * contains request with sequence number <code>lowestSeqNo</code>.
     * <p>
     * Example: Given startingSeqNo containing:
     * 
     * <pre>
     * [0, 0]
     * [1, 5]
     * [2, 10]
     * [3, 15]
     * [4, 20]
     * </pre>
     * After truncating to instance 12, startingSeqNo will contain:
     * 
     * <pre>
     * [2, 10]
     * [3, 15]
     * [4, 20]
     * </pre>
     * 
     * <pre>
     * 10 <= 12 < 15
     * </pre>
     * 
     * @param lowestSeqNo
     */
    private void truncateStartingSeqNo(int lowestSeqNo) {
        Pair<Integer, Integer> previous = null;
        while (!startingSeqNo.isEmpty() && startingSeqNo.getFirst().getValue() <= lowestSeqNo) {
            previous = startingSeqNo.pollFirst();
        }

        if (previous != null) {
            startingSeqNo.addFirst(previous);
        }
    }

    private final static Logger logger = Logger.getLogger(ServiceProxy.class.getCanonicalName());
}

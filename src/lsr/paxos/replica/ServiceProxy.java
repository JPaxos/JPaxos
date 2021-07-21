package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.ClientRequest;
import lsr.common.CrashModel;
import lsr.common.Pair;
import lsr.common.Reply;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotMaintainer;
import lsr.paxos.UnBatcher;
import lsr.paxos.NATIVE.PersistentMemory;
import lsr.paxos.replica.storage.InMemoryServiceProxyStorage;
import lsr.paxos.replica.storage.ReplicaStorage;
import lsr.paxos.replica.storage.ServiceProxyStorage;
import lsr.paxos.replica.storage.SnapshotlyPersistentServiceProxyStorage;
import lsr.paxos.storage.PersistentStorage;
import lsr.paxos.storage.Storage;
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
 * </pre> The first consensus instance contains 2 requests, second contains 3
 * requests and the last instance contains only one request. It is important
 * that we call <code>execute()</code> method on underlying service with
 * following arguments:
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
 * </pre> Update service from snapshot:
 * 
 * <pre>
 * Snapshot snapshot = ...; // from paxos protocol or from disc
 * 
 * proxy.updateToSnapshot(snapshot);
 * </pre>
 * 
 * @see Service
 */
public class ServiceProxy implements SnapshotListener {

    protected final ServiceProxyStorage storage;

    /** Used for keeping requestId for snapshot purposes. */
    private ClientRequest currentRequest;

    private final Service service;
    private final List<SnapshotListener2> listeners = new ArrayList<SnapshotListener2>();
    private final SingleThreadDispatcher replicaDispatcher;

    private ReplicaStorage replicaStorage;

    private SnapshotMaintainer snapshotMaintainer;

    /**
     * Creates new <code>ServiceProxy</code> instance.
     * 
     * @param service - the service wrapped by this proxy
     * @param replicaStorage - the cache of responses from service
     * @param replicaDispatcher - the dispatcher used in replica
     */
    public ServiceProxy(Service service, ReplicaStorage replicaStorage,
                        SingleThreadDispatcher replicaDispatcher) {
        this.service = service;
        this.replicaDispatcher = replicaDispatcher;
        service.addSnapshotListener(this);
        this.replicaStorage = replicaStorage;

        if (processDescriptor.crashModel == CrashModel.Pmem) {
            // FIXME: that ugly way of getting last snapshot is OOP wrong.
            storage = new SnapshotlyPersistentServiceProxyStorage(
                    new PersistentStorage().getLastSnapshot());
        } else
            storage = new InMemoryServiceProxyStorage();
    }

    public void setSnapshotMaintainer(SnapshotMaintainer snapshotMaintainer) {
        this.snapshotMaintainer = snapshotMaintainer;
    }

    /**
     * Executes the request on underlying service with correct sequence number.
     * 
     * @param request - the request to execute on service
     * @return the reply from service
     */
    public byte[] execute(ClientRequest request) {
        storage.incNextSeqNo();
        if (storage.decSkip()) {
            logger.debug(processDescriptor.logMark_OldBenchmark,
                    "Skipping request {} at seqno {}", request, storage.getNextSeqNo() - 1);
            return storage.getNextSkippedRequestResponse();
        } else {
            currentRequest = request;
            long nanos = 0;
            if (logger.isDebugEnabled(processDescriptor.logMark_OldBenchmark)) {
                logger.debug(processDescriptor.logMark_OldBenchmark,
                        "Passing request to be executed to service: {} as {}", request,
                        (storage.getNextSeqNo() - 1));
                nanos = System.nanoTime();
            }
            byte[] result = service.execute(request.getValue(), storage.getNextSeqNo() - 1);
            if (logger.isDebugEnabled(processDescriptor.logMark_OldBenchmark)) {
                nanos = System.nanoTime() - nanos;
                logger.debug(processDescriptor.logMark_OldBenchmark,
                        "Request {} execution took {} nanoseconds", request, nanos);
            }
            if (logger.isTraceEnabled(processDescriptor.logMark_Benchmark2019nope))
                logger.trace(processDescriptor.logMark_Benchmark2019nope, "XX {}",
                        storage.getNextSeqNo() - 1);
            return result;
        }
    }

    /**
     * Notifies this service proxy that all request from specified consensus
     * instance has been executed.
     * 
     * @param instanceId - the id of executed consensus instance
     */
    public void instanceExecuted(int instanceId) {
        storage.addStartingSeqenceNo(instanceId + 1, storage.getNextSeqNo());
    }

    /**
     * Notifies underlying service that it would be good to create snapshot now.
     * <code>Service</code> should check whether this is good moment, and create
     * snapshot if needed.
     */
    public void askForSnapshot() {
        if (!storage.hasSkip())
            service.askForSnapshot(storage.getLastSnapshotNextSeqNo());
        else
            snapshotMaintainer.resetAskForceSnapshot();
    }

    /**
     * Notifies underlying service that size of logs are much bigger than
     * estimated size of snapshot. Not implementing this method may cause
     * slowing down the algorithm, especially in case of network problems and
     * also recovery in case of crash can take more time.
     */
    public void forceSnapshot() {
        if (!storage.hasSkip())
            service.forceSnapshot(storage.getLastSnapshotNextSeqNo());
        else
            snapshotMaintainer.resetAskForceSnapshot();
    }

    /**
     * Updates states of underlying service to specified snapshot.
     * 
     * @param snapshot - the snapshot with newer service state
     */
    public void updateToSnapshot(Snapshot snapshot) {
        int seqNoBeforeSnapshot = storage.getNextSeqNo();
        storage.setLastSnapshotNextSeqNo(snapshot.getNextRequestSeqNo());
        storage.setNextSeqNo(snapshot.getStartingRequestSeqNo());
        storage.setSkip(snapshot.getNextRequestSeqNo() - snapshot.getStartingRequestSeqNo());

        storage.setSkippedCache(snapshot.getPartialResponseCache());

        assert snapshot.getPartialResponseCache().size() == snapshot.getNextRequestSeqNo() -
                                                            snapshot.getStartingRequestSeqNo();

        if (seqNoBeforeSnapshot > snapshot.getStartingRequestSeqNo()) {
            logger.error(
                    "The ServiceProxy forced to recover from a snapshot older than the current state. ServiceSeqNo:{}  SnapshotSeqNo:{}",
                    seqNoBeforeSnapshot, snapshot.getStartingRequestSeqNo());
            storage.truncateStartingSeqNo(storage.getNextSeqNo());
        } else {
            storage.clearStartingSeqNo();
            storage.addStartingSeqenceNo(
                    snapshot.getNextInstanceId(),
                    snapshot.getStartingRequestSeqNo());
        }

        service.updateToSnapshot(snapshot.getNextRequestSeqNo(), snapshot.getValue());
    }

    public void onSnapshotMade(final int nextRequestSeqNo, final byte[] value,
                               final byte[] response) {
        replicaDispatcher.executeAndWait(new Runnable() {
            public void run() {
                if (value == null) {
                    throw new IllegalArgumentException("The snapshot value cannot be null");
                }
                if (nextRequestSeqNo < storage.getLastSnapshotNextSeqNo()) {
                    throw new IllegalArgumentException("The snapshot is older than previous. " +
                                                       "Next: " + nextRequestSeqNo + ", Last: " +
                                                       storage.getLastSnapshotNextSeqNo());
                }
                if (nextRequestSeqNo > storage.getNextSeqNo()) {
                    throw new IllegalArgumentException(
                            "The snapshot marked as newer than current state. " +
                                                       "nextRequestSeqNo: " + nextRequestSeqNo +
                                                       ", nextSeqNo: " +
                                                       storage.getNextSeqNo());
                }

                try {
                    if (processDescriptor.crashModel == CrashModel.Pmem)
                        PersistentMemory.startThreadLocalTx();

                    storage.truncateStartingSeqNo(nextRequestSeqNo);
                    Pair<Integer, Integer> nextInstanceEntry = storage.getFrontStartingSeqNo();
                    assert nextInstanceEntry.getValue() <= nextRequestSeqNo : "NextInstance: " +
                                                                              nextInstanceEntry.getValue() +
                                                                              ", nextReqSeqNo: " +
                                                                              nextRequestSeqNo;

                    Snapshot snapshot = new Snapshot();

                    snapshot.setNextRequestSeqNo(nextRequestSeqNo);
                    snapshot.setNextInstanceId(nextInstanceEntry.getKey());
                    snapshot.setStartingRequestSeqNo(nextInstanceEntry.getValue());
                    snapshot.setValue(value);

                    final int localSkip = snapshot.getNextRequestSeqNo() -
                                          snapshot.getStartingRequestSeqNo();

                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Service created a snapshot. Next request is {}, thr preceding request is in instance {} (that starts at seqno {})",
                                nextRequestSeqNo, snapshot.getNextInstanceId(),
                                snapshot.getStartingRequestSeqNo());
                    }

                    List<Reply> thisInstanceReplies = replicaStorage.getRepliesInInstance(
                            snapshot.getNextInstanceId());
                    if (thisInstanceReplies == null) {
                        assert localSkip == 0 ||
                               snapshot.getStartingRequestSeqNo() == nextRequestSeqNo - 1;
                        if (localSkip == 0)
                            snapshot.setPartialResponseCache(new ArrayList<Reply>(0));
                        else {
                            assert localSkip == 1;
                            snapshot.setPartialResponseCache(Collections.singletonList(
                                    new Reply(currentRequest.getRequestId(), response)));
                        }
                    } else {
                        boolean hasLastResponse;
                        if (thisInstanceReplies.size() < localSkip) {
                            assert thisInstanceReplies.size() == localSkip - 1;
                            hasLastResponse = false;
                            snapshot.setPartialResponseCache(new ArrayList<Reply>(
                                    thisInstanceReplies.subList(0, localSkip - 1)));
                        } else {
                            snapshot.setPartialResponseCache(new ArrayList<Reply>(
                                    thisInstanceReplies.subList(0, localSkip)));
                            hasLastResponse = true;
                        }

                        if (!hasLastResponse) {
                            if (response == null) {
                                throw new IllegalArgumentException(
                                        "If snapshot is executed from within execute() " +
                                                                   "for current request, the response has to be " +
                                                                   "given with snapshot\n");
                            }
                            snapshot.getPartialResponseCache().add(
                                    new Reply(currentRequest.getRequestId(), response));
                        }
                    }

                    storage.onSnapshotMade(snapshot);

                    assert snapshot.getPartialResponseCache().size() == snapshot.getNextRequestSeqNo() -
                                                                        snapshot.getStartingRequestSeqNo();

                    for (SnapshotListener2 listener : listeners) {
                        listener.onSnapshotMade(snapshot);
                    }
                } finally {
                    if (processDescriptor.crashModel == CrashModel.Pmem)
                        PersistentMemory.commitThreadLocalTx();
                }
            }
        });
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
     * Registers new listener which will be called every time new snapshot is
     * created by underlying <code>Service</code>.
     * 
     * @param listener - the listener to register
     */
    public void addSnapshotListener(SnapshotListener2 listener) {
        listeners.add(listener);
    }

    /**
     * Unregisters the listener from this network. It will not be called when
     * new snapshot is created by this <code>Service</code>.
     * 
     * @param listener - the listener to unregister
     */
    public void removeSnapshotListener(SnapshotListener2 listener) {
        listeners.add(listener);
    }

    /** With pmem, the JPaxos starts with state same as at crash, however the */
    public void doPmemRecovery(Storage paxosStorage) {

        int redoNextSeqNo;
        int redoInstId;
        int skip;

        Snapshot snapshot = paxosStorage.getLastSnapshot();
        if (snapshot != null) {
            logger.info(
                    "Bringing back the service to {}", snapshot.toString());
            service.updateToSnapshot(snapshot.getNextRequestSeqNo(), snapshot.getValue());

            redoNextSeqNo = snapshot.getNextRequestSeqNo();
            redoInstId = snapshot.getNextInstanceId();
            skip = snapshot.getNextRequestSeqNo() - snapshot.getStartingRequestSeqNo();

            if (logger.isTraceEnabled()) {
                logger.trace(snapshot.dump());
            }

            assert skip == snapshot.getPartialResponseCache().size();
        } else {
            redoInstId = 0;
            redoNextSeqNo = 0;
            skip = 0;
        }

        assert snapshot == null ||
               redoNextSeqNo == storage.getLastSnapshotNextSeqNo() : redoNextSeqNo + " vs " +
                                                                     storage.getLastSnapshotNextSeqNo();

        logger.debug("snapshot next seqno: " + redoNextSeqNo + ", storage next seqno: " +
                     storage.getNextSeqNo() + ", skip: " + skip);

        if (redoNextSeqNo == storage.getNextSeqNo() + skip) {
            logger.info("Service next seqNo is already at " + redoNextSeqNo);
            if (skip != 0) {
                logger.debug("Setting skip to " + skip);
                storage.setSkippedCache(snapshot.getPartialResponseCache());
                storage.setSkip(skip);
            }
            return;
        }

        while (true) {
            logger.debug("Re-doing at recovery requests from instace " + redoInstId);

            ClientRequest[] crs = UnBatcher.unpackCR(
                    paxosStorage.getLog().getInstance(redoInstId).getValue());
            for (ClientRequest cr : crs) {
                if (skip > 0) {
                    // request already execcuted in state snapshot
                    skip--;
                    continue;
                }
                logger.debug(
                        "Passing request to be executed to service: {} as {} (redo at recovery)",
                        cr, redoNextSeqNo);
                byte[] reply = service.execute(cr.getValue(), redoNextSeqNo);
                redoNextSeqNo++;
                if (redoNextSeqNo == storage.getNextSeqNo()) {
                    replicaStorage.setLastReplyForClient(redoInstId,
                            cr.getRequestId().getClientId(), new Reply(cr.getRequestId(), reply));

                    logger.info("Service next seqNo is back " + redoNextSeqNo);
                    assert skip == 0;
                    return;
                }
            }
            redoInstId++;
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(ServiceProxy.class);
}

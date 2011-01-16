package lsr.paxos.replica;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Snapshot;
import lsr.service.Service;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ServiceProxyTest {
    private SingleThreadDispatcher dispatcher;
    private Service service;
    private Map<Integer, List<Reply>> responsesCache;
    private ServiceProxy serviceProxy;

    @Before
    public void setUp() {
        dispatcher = mock(SingleThreadDispatcher.class);
        service = mock(Service.class);
        responsesCache = new HashMap<Integer, List<Reply>>();
        serviceProxy = new ServiceProxy(service, responsesCache, dispatcher);
    }

    @Test
    public void shouldExecuteSingleRequestOnService() {
        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        serviceProxy.execute(request);

        verify(service).execute(new byte[] {1, 2, 3}, 0);
    }

    @Test
    public void shouldExecuteRequestsOnService() {
        Request request2 = new Request(new RequestId(2, 2), new byte[] {2});
        Request request1 = new Request(new RequestId(1, 1), new byte[] {1});

        serviceProxy.execute(request1);
        serviceProxy.instanceExecuted(0);
        serviceProxy.execute(request2);
        serviceProxy.instanceExecuted(1);

        verify(service).execute(new byte[] {1}, 0);
        verify(service).execute(new byte[] {2}, 1);
    }

    @Test
    public void shouldUpdateToSnapshot() {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextRequestSeqNo(7);
        snapshot.setNextInstanceId(2);
        snapshot.setStartingRequestSeqNo(5);
        snapshot.setValue(new byte[] {1, 2, 3});
        snapshot.setPartialResponseCache(new ArrayList<Reply>());

        serviceProxy.updateToSnapshot(snapshot);

        verify(service).updateToSnapshot(7, snapshot.getValue());
    }

    @Test
    public void shouldSkipRequestsAfterUpdatingToSnapshot() {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextRequestSeqNo(7);
        snapshot.setNextInstanceId(2);
        snapshot.setStartingRequestSeqNo(5);
        snapshot.setValue(new byte[] {1, 2, 3});
        Reply reply1 = new Reply(new RequestId(0, 0), new byte[] {1, 2, 3});
        Reply reply2 = new Reply(new RequestId(1, 1), new byte[] {1, 2, 3, 4});
        snapshot.setPartialResponseCache(Arrays.asList(reply1, reply2));

        serviceProxy.updateToSnapshot(snapshot);
        verify(service).updateToSnapshot(7, snapshot.getValue());
        byte[] skipped1 = serviceProxy.execute(RequestGenerator.generate());
        byte[] skipped2 = serviceProxy.execute(RequestGenerator.generate());

        assertArrayEquals(reply1.getValue(), skipped1);
        assertArrayEquals(reply2.getValue(), skipped2);
        verify(service, never()).execute(any(byte[].class), anyInt());

        Request firstExecuted = RequestGenerator.generate();
        serviceProxy.execute(firstExecuted);
        verify(service).execute(firstExecuted.getValue(), 7);
    }

    @Test
    public void shouldAskServiceForSnapshotWithoutPreviousSnapshot() {
        serviceProxy.askForSnapshot();
        verify(service).askForSnapshot(-1);
    }

    @Test
    public void shouldAskServiceForSnapshotWithPreviousSnapshot() {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextRequestSeqNo(10);
        snapshot.setNextInstanceId(111);
        snapshot.setPartialResponseCache(new ArrayList<Reply>());

        serviceProxy.updateToSnapshot(snapshot);

        serviceProxy.askForSnapshot();
        verify(service).askForSnapshot(10);
    }

    @Test
    public void shouldForceSnapshotWithoutPreviousSnapshost() {
        serviceProxy.forceSnapshot();
        verify(service).forceSnapshot(-1);
    }

    @Test
    public void shouldForceSnapshotWithPreviousSnapshot() {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextRequestSeqNo(10);
        snapshot.setNextInstanceId(111);
        snapshot.setPartialResponseCache(new ArrayList<Reply>());

        serviceProxy.updateToSnapshot(snapshot);

        serviceProxy.forceSnapshot();
        verify(service).forceSnapshot(10);
    }

    @Test
    public void shouldRecoveryFinished() {
        serviceProxy.recoveryFinished();
        verify(service).recoveryFinished();
    }

    @Test
    public void shouldRegisterSnapshotListener() {
        verify(service).addSnapshotListener(serviceProxy);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionOnNullSnapshotValue() {
        serviceProxy.onSnapshotMade(1, null, null);
        executeDispatcher();
    }

    @Test
    public void shouldHandleSnapshotFromService() {
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(0);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(1);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        responsesCache.put(2, Arrays.asList(ReplyGenerator.generate(), ReplyGenerator.generate(),
                ReplyGenerator.generate()));
        SnapshotListener2 snapshotListener2 = mock(SnapshotListener2.class);
        serviceProxy.addSnapshotListener(snapshotListener2);

        serviceProxy.onSnapshotMade(8, new byte[] {1, 2, 3}, null);
        executeDispatcher();

        ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
        verify(snapshotListener2).onSnapshotMade(snapshotCaptor.capture());
        Snapshot snapshot = snapshotCaptor.getValue();
        assertArrayEquals(new byte[] {1, 2, 3}, snapshot.getValue());
        assertEquals(2, snapshot.getNextInstanceId());
        assertEquals(8, snapshot.getNextRequestSeqNo());
        assertEquals(5, snapshot.getStartingRequestSeqNo());
        assertEquals(responsesCache.get(2), snapshot.getPartialResponseCache());
    }

    @Test
    public void shouldHandleSnapshotFromServiceAfterInstanceIsExecuted() {
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(0);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(1);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(2);
        SnapshotListener2 snapshotListener2 = mock(SnapshotListener2.class);
        serviceProxy.addSnapshotListener(snapshotListener2);

        serviceProxy.onSnapshotMade(8, new byte[] {1, 2, 3}, null);
        executeDispatcher();

        ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
        verify(snapshotListener2).onSnapshotMade(snapshotCaptor.capture());
        Snapshot snapshot = snapshotCaptor.getValue();
        assertArrayEquals(new byte[] {1, 2, 3}, snapshot.getValue());
        assertEquals(3, snapshot.getNextInstanceId());
        assertEquals(8, snapshot.getNextRequestSeqNo());
        assertEquals(8, snapshot.getStartingRequestSeqNo());
        assertEquals(0, snapshot.getPartialResponseCache().size());
    }

    @Test
    public void shouldHandleSnapshotFromServiceCalledWithinExecuteMethod() {
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(0);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(1);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        SnapshotListener2 snapshotListener2 = mock(SnapshotListener2.class);
        serviceProxy.addSnapshotListener(snapshotListener2);
        responsesCache.put(2, Arrays.asList(ReplyGenerator.generate(), ReplyGenerator.generate()));

        Request lastRequest = RequestGenerator.generate();
        serviceProxy.execute(lastRequest);
        serviceProxy.onSnapshotMade(8, new byte[] {1, 2, 3}, new byte[] {1, 2, 3, 4});
        executeDispatcher();

        ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
        verify(snapshotListener2).onSnapshotMade(snapshotCaptor.capture());
        Snapshot snapshot = snapshotCaptor.getValue();
        assertArrayEquals(new byte[] {1, 2, 3}, snapshot.getValue());
        assertEquals(2, snapshot.getNextInstanceId());
        assertEquals(8, snapshot.getNextRequestSeqNo());
        assertEquals(5, snapshot.getStartingRequestSeqNo());
        assertEquals(3, snapshot.getPartialResponseCache().size());
    }

    @Test
    public void shouldHandleSnapshotFromPast() {
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(0);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(1);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(2);
        SnapshotListener2 snapshotListener2 = mock(SnapshotListener2.class);
        serviceProxy.addSnapshotListener(snapshotListener2);
        responsesCache.put(0, Arrays.asList(ReplyGenerator.generate(), ReplyGenerator.generate(),
                ReplyGenerator.generate()));

        serviceProxy.onSnapshotMade(1, new byte[] {1, 2, 3}, null);
        executeDispatcher();

        ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
        verify(snapshotListener2).onSnapshotMade(snapshotCaptor.capture());
        Snapshot snapshot = snapshotCaptor.getValue();
        assertArrayEquals(new byte[] {1, 2, 3}, snapshot.getValue());
        assertEquals(0, snapshot.getNextInstanceId());
        assertEquals(1, snapshot.getNextRequestSeqNo());
        assertEquals(0, snapshot.getStartingRequestSeqNo());
        assertEquals(1, snapshot.getPartialResponseCache().size());
    }

    @Test
    public void shouldHandleSnapshotAfterUpdateToSnapshot() {
        SnapshotListener2 snapshotListener2 = mock(SnapshotListener2.class);
        serviceProxy.addSnapshotListener(snapshotListener2);
        Snapshot snapshot = new Snapshot();
        snapshot.setNextRequestSeqNo(5);
        snapshot.setNextInstanceId(2);
        snapshot.setStartingRequestSeqNo(5);
        snapshot.setValue(new byte[] {1, 2, 3});
        snapshot.setPartialResponseCache(new ArrayList<Reply>());

        serviceProxy.updateToSnapshot(snapshot);
        serviceProxy.onSnapshotMade(5, new byte[] {1, 2, 3}, null);
        executeDispatcher();

        ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
        verify(snapshotListener2).onSnapshotMade(snapshotCaptor.capture());
        Snapshot snapshot1 = snapshotCaptor.getValue();
        assertArrayEquals(new byte[] {1, 2, 3}, snapshot1.getValue());
        assertEquals(2, (int) snapshot1.getNextInstanceId());
        assertEquals(5, snapshot1.getNextRequestSeqNo());
        assertEquals(5, snapshot1.getStartingRequestSeqNo());
        assertEquals(0, snapshot1.getPartialResponseCache().size());
    }

    @Test
    public void shouldUpdateToSnapshotFromBeforeLastExecuted() {
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(0);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(1);
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.execute(RequestGenerator.generate());
        serviceProxy.instanceExecuted(2);

        Snapshot snapshot = new Snapshot();
        snapshot.setNextRequestSeqNo(1);
        snapshot.setNextInstanceId(0);
        snapshot.setStartingRequestSeqNo(0);
        snapshot.setValue(new byte[] {1, 2, 3});
        snapshot.setPartialResponseCache(new ArrayList<Reply>());

        serviceProxy.updateToSnapshot(snapshot);

        verify(service).updateToSnapshot(1, snapshot.getValue());
    }

    private void executeDispatcher() {
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(dispatcher).executeAndWait(task.capture());
        task.getValue().run();
    }
}

class ReplyGenerator {
    private static int id = 0;

    public static Reply generate() {
        id++;
        return new Reply(new RequestId(id, id), new byte[] {1, 2, 3});
    }
}

class RequestGenerator {
    private static int id = 0;

    public static Request generate() {
        id++;
        return new Request(new RequestId(id, id), new byte[] {(byte) id});
    }
}

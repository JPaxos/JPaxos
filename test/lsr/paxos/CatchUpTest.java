package lsr.paxos;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.SortedMap;
import java.util.TreeMap;

import lsr.common.Config;
import lsr.common.ProcessDescriptor;
import lsr.common.ProcessDescriptorHelper;
import lsr.common.Range;
import lsr.paxos.messages.CatchUpQuery;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;
import lsr.paxos.recovery.MockDispatcher;
import lsr.paxos.recovery.MockNetwork;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CatchUpTest {
    private SnapshotProvider snapshotProvider;
    private Network network;
    private Paxos paxos;
    private Storage storage;
    private Log log;
    private MockDispatcher dispatcher;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);

        snapshotProvider = mock(SnapshotProvider.class);
        network = mock(Network.class);
        paxos = mock(Paxos.class);
        storage = mock(Storage.class);
        log = mock(Log.class);
        dispatcher = new MockDispatcher();

        when(paxos.getDispatcher()).thenReturn(dispatcher);
        when(storage.getLog()).thenReturn(log);
    }

    @Test
    public void shouldSendCatchUpQuery() {
        // instances [0, 9] are decided, [10, 100] are UNKNOWN
        initializeLog(10, 100);

        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        when(storage.getView()).thenReturn(5);

        CatchUp catchUp = new CatchUp(snapshotProvider, paxos, storage, network);
        catchUp.start();

        dispatcher.advanceTime(Config.PERIODIC_CATCHUP_TIMEOUT);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(1));

        CatchUpQuery query = (CatchUpQuery) messageArgument.getValue();
        assertEquals(5, query.getView());
        assertEquals(new Range(10, 99), query.getInstanceIdRangeArray()[0]);
        assertArrayEquals(new int[] {100}, query.getInstanceIdArray());
        assertEquals(false, query.isPeriodicQuery());
    }

    @Test
    public void shouldNotSendCatchUpQueryAsLeader() {
        // instances [0, 9] are decided, [10, 99] are UNKNOWN
        initializeLog(10, 100);

        when(paxos.getLeaderId()).thenReturn(0);
        when(paxos.isLeader()).thenReturn(true);
        when(storage.getView()).thenReturn(3);

        CatchUp catchUp = new CatchUp(snapshotProvider, paxos, storage, network);
        catchUp.start();

        dispatcher.advanceTime(Config.PERIODIC_CATCHUP_TIMEOUT);
        dispatcher.execute();

        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    @Test
    public void shouldNotSendCatchUpQueryWhenInstanceInWindow() {
        assertEquals(2, ProcessDescriptor.getInstance().windowSize);

        // instances [0, 9] are decided, [10, 11] is UNKNOWN
        initializeLog(10, 12);

        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        when(storage.getView()).thenReturn(5);

        CatchUp catchUp = new CatchUp(snapshotProvider, paxos, storage, network);
        catchUp.start();

        dispatcher.advanceTime(Config.PERIODIC_CATCHUP_TIMEOUT);
        dispatcher.execute();

        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    @Test
    public void shouldSendPeriodicCatchUpQuery() {
        assertEquals(2, ProcessDescriptor.getInstance().windowSize);

        // instances [0, 9] are decided, [10, 11] is UNKNOWN
        initializeLog(10, 13);

        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        when(storage.getView()).thenReturn(5);

        CatchUp catchUp = new CatchUp(snapshotProvider, paxos, storage, network);
        catchUp.start();

        dispatcher.advanceTime(Config.PERIODIC_CATCHUP_TIMEOUT);
        dispatcher.execute();

        initializeLog(13, 13);
        assertEquals(storage.getFirstUncommitted(), storage.getLog().getNextId());
        dispatcher.advanceTime(Config.PERIODIC_CATCHUP_TIMEOUT);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(2)).sendMessage(messageArgument.capture(), anyInt());
        CatchUpQuery query = (CatchUpQuery) messageArgument.getAllValues().get(1);

        assertEquals(true, query.isPeriodicQuery());
        assertArrayEquals(new int[] {13}, query.getInstanceIdArray());
        assertEquals(0, query.getInstanceIdRangeArray().length);
    }

    @Test
    public void shouldSendQueryWithFewIdRanges() {
        // instances [0, 2], 4, 6, [8, 10]
        when(storage.getFirstUncommitted()).thenReturn(0);
        when(log.getNextId()).thenReturn(11);
        SortedMap<Integer, ConsensusInstance> instanceMap =
                new TreeMap<Integer, ConsensusInstance>();

        instanceMap.put(0, new ConsensusInstance(0));
        instanceMap.put(1, new ConsensusInstance(1));
        instanceMap.put(2, new ConsensusInstance(2));
        instanceMap.put(3, new ConsensusInstance(3, LogEntryState.DECIDED, 1, new byte[] {1}));
        instanceMap.put(4, new ConsensusInstance(4));
        instanceMap.put(5, new ConsensusInstance(5, LogEntryState.DECIDED, 1, new byte[] {1}));
        instanceMap.put(6, new ConsensusInstance(6));
        instanceMap.put(7, new ConsensusInstance(7, LogEntryState.DECIDED, 1, new byte[] {1}));
        instanceMap.put(8, new ConsensusInstance(8));
        instanceMap.put(9, new ConsensusInstance(9));
        instanceMap.put(10, new ConsensusInstance(10));
        when(log.getInstanceMap()).thenReturn(instanceMap);

        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        when(storage.getView()).thenReturn(5);

        CatchUp catchUp = new CatchUp(snapshotProvider, paxos, storage, network);
        catchUp.start();

        dispatcher.advanceTime(Config.PERIODIC_CATCHUP_TIMEOUT);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(1));

        CatchUpQuery query = (CatchUpQuery) messageArgument.getValue();
        assertEquals(5, query.getView());
        assertEquals(new Range(0, 2), query.getInstanceIdRangeArray()[0]);
        assertEquals(new Range(8, 10), query.getInstanceIdRangeArray()[1]);
        assertArrayEquals(new int[] {4, 6, 11}, query.getInstanceIdArray());
        assertEquals(false, query.isPeriodicQuery());
    }

    @Test
    public void shouldRespondToCatchUpQuery() {
        initializeLog(5, 10);
        
        MockNetwork mockNetwork = new MockNetwork();

        new CatchUp(snapshotProvider, paxos, storage, network);

        CatchUpQuery query = new CatchUpQuery(
                1,
                new int[] {3, 5, 7},
                new Range[] {new Range(9, 11)});
        mockNetwork.fireReceive(query, 1);
        dispatcher.execute();
        
        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    private void initializeLog(int firstUncommitted, int nextId) {
        when(storage.getFirstUncommitted()).thenReturn(firstUncommitted);
        when(log.getNextId()).thenReturn(nextId);
        SortedMap<Integer, ConsensusInstance> instanceMap =
                new TreeMap<Integer, ConsensusInstance>();
        for (int i = 0; i < firstUncommitted; i++) {
            ConsensusInstance instance = new ConsensusInstance(i, LogEntryState.DECIDED, 1,
                    new byte[] {1, 2, 3});
            instance.setDecided();
            instanceMap.put(i, instance);
        }
        for (int i = firstUncommitted; i < nextId; i++) {
            ConsensusInstance instance = new ConsensusInstance(i);
            instanceMap.put(i, instance);
        }
        when(log.getInstanceMap()).thenReturn(instanceMap);
    }
}

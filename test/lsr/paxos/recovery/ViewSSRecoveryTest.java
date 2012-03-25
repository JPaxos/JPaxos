package lsr.paxos.recovery;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.BitSet;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.CatchUp;
import lsr.paxos.CatchUpListener;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.storage.SingleNumberWriter;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ViewSSRecoveryTest {
    private DecideCallback decideCallback;
    private SnapshotProvider snapshotProvider;
    private ViewSSRecovery recovery;
    private RecoveryListener listener;
    private MockDispatcher dispatcher;
    private MockNetwork network;
    private CatchUp catchUp;
    private SingleNumberWriter writer;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);

        decideCallback = mock(DecideCallback.class);
        snapshotProvider = mock(SnapshotProvider.class);
        listener = mock(RecoveryListener.class);
        catchUp = mock(CatchUp.class);
        dispatcher = new MockDispatcher();
        network = mock(MockNetwork.class);
        when(network.fireReceive(any(Message.class), anyInt())).thenCallRealMethod();
        writer = mock(SingleNumberWriter.class);
    }

    @Test
    public void shouldRecoverInstanltyOnFirstRun() throws IOException {
        setInitialView(0);
        initializeRecovery();

        verify(listener).recoveryFinished();

        Storage storage = recovery.getPaxos().getStorage();
        assertEquals(1, storage.getView());
        verify(writer).writeNumber(1);
    }

    @Test
    public void shouldRespondToRecoveryMessageWhenRecovered() throws IOException {
        setInitialView(0);
        initializeRecovery();

        verify(listener).recoveryFinished();

        Recovery recovery = new Recovery(0, -1);
        network.fireReceive(recovery, 1);
        dispatcher.execute();
        verify(network).sendMessage(any(Message.class), eq(1));
    }

    @Test
    public void shouldRetransmitRecoveryMessageToAll() throws IOException {
        setInitialView(1);
        initializeRecovery();

        // all except local
        BitSet all = new BitSet(3);
        all.set(1, 3);

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(all));
        Recovery recoveryMessage = (Recovery) messageArgument.getValue();
        assertEquals(1, recoveryMessage.getView());

        dispatcher.advanceTime(3000);
        dispatcher.execute();

        messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(2)).sendMessage(messageArgument.capture(), eq(all));
        recoveryMessage = (Recovery) messageArgument.getValue();
        assertEquals(1, recoveryMessage.getView());
    }

    @Test
    public void shouldStopTransmittingRecoveryAfterRecevingRecoveryAnswer() throws IOException {
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 12), 1);
        dispatcher.execute();
        reset(network);

        dispatcher.advanceTime(3000);
        dispatcher.execute();

        BitSet destinations = new BitSet(3);
        destinations.set(2);
        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(destinations));
        Recovery recovery = (Recovery) messageArgument.getValue();
        assertEquals(1, recovery.getView());
    }

    @Test
    public void shouldWaitForMajorityOfRecoveryAnswer() throws IOException {
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, 12), 2);
        dispatcher.execute();

        catchUp(12);

        verify(listener).recoveryFinished();

        Storage storage = recovery.getPaxos().getStorage();
        assertEquals(5, storage.getView());
    }

    @Test
    public void shouldUpdateLogBeforeCatchUp() throws IOException {
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, 12), 2);
        dispatcher.execute();

        Storage storage = recovery.getPaxos().getStorage();
        assertEquals(12, storage.getLog().getNextId());
    }

    @Test
    public void shouldForceCatchUp() throws IOException {
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, 12), 2);
        dispatcher.execute();

        catchUp(10);
        verify(catchUp).forceCatchup();
        catchUp(12);

        verify(listener).recoveryFinished();

        Storage storage = recovery.getPaxos().getStorage();
        assertEquals(5, storage.getView());
    }

    @Test
    public void shouldRecoverWhenNoInstanceWasDecided() throws IOException {
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, 0), 1);
        network.fireReceive(new RecoveryAnswer(5, 0), 2);
        dispatcher.execute();

        verify(catchUp, times(0)).start();
        verify(listener).recoveryFinished();

        Storage storage = recovery.getPaxos().getStorage();
        assertEquals(0, storage.getLog().getNextId());
    }

    @Test
    public void shouldWaitForMessageFromLeaderAndRetransmittToAll() throws IOException {
        ProcessDescriptorHelper.initialize(5, 0);
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(4, 0), 1);
        network.fireReceive(new RecoveryAnswer(4, 0), 2);
        network.fireReceive(new RecoveryAnswer(4, 0), 3);
        dispatcher.execute();

        dispatcher.advanceTime(3000);
        dispatcher.execute();

        BitSet all = new BitSet();
        all.set(1, 5);
        verify(network, times(2)).sendMessage(any(Message.class), eq(all));

        verify(listener, never()).recoveryFinished();
    }

    @Test
    public void shouldWaitForMessageFromLeaderOfNewestView() throws IOException {
        ProcessDescriptorHelper.initialize(5, 0);
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(9, 0), 2);
        network.fireReceive(new RecoveryAnswer(9, 0), 3);
        network.fireReceive(new RecoveryAnswer(6, 0), 1);
        dispatcher.execute();

        dispatcher.advanceTime(3000);
        dispatcher.execute();

        BitSet all = new BitSet();
        all.set(1, 5);
        verify(network, times(2)).sendMessage(any(Message.class), eq(all));

        verify(listener, never()).recoveryFinished();
    }

    @Test
    public void shouldWaitForMessageFromLeader() throws IOException {
        ProcessDescriptorHelper.initialize(5, 0);
        setInitialView(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(6, 0), 1);
        network.fireReceive(new RecoveryAnswer(9, 0), 2);
        network.fireReceive(new RecoveryAnswer(9, 0), 3);
        dispatcher.execute();

        dispatcher.advanceTime(3000);
        dispatcher.execute();

        BitSet all = new BitSet();
        all.set(1, 5);
        verify(network, times(2)).sendMessage(any(Message.class), eq(all));

        verify(listener, never()).recoveryFinished();
    }

    private void catchUp(int instanceId) {
        ArgumentCaptor<CatchUpListener> catchUpListener =
                ArgumentCaptor.forClass(CatchUpListener.class);
        verify(catchUp).addListener(catchUpListener.capture());
        when(catchUp.removeListener(any(CatchUpListener.class))).thenReturn(true);

        Storage storage = recovery.getPaxos().getStorage();
        for (int i = 0; i < instanceId; i++) {
            storage.getLog().getInstance(i).setValue(2, new byte[] {1, 2});
            storage.getLog().getInstance(i).setDecided();
        }
        storage.updateFirstUncommitted();
        catchUpListener.getValue().catchUpSucceeded();
    }

    private void initializeRecovery() throws IOException {
        recovery = new ViewSSRecovery(snapshotProvider, decideCallback, writer) {
            protected Paxos createPaxos(DecideCallback decideCallback,
                                        SnapshotProvider snapshotProvider, Storage storage)
                    throws IOException {
                Paxos paxos = mock(Paxos.class);
                when(paxos.getStorage()).thenReturn(storage);
                when(paxos.getNetwork()).thenReturn(network);
                when(paxos.getDispatcher()).thenReturn(dispatcher);
                when(paxos.getCatchup()).thenReturn(catchUp);
                return paxos;
            }
        };
        recovery.addRecoveryListener(listener);
        recovery.start();
        dispatcher.execute();
    }

    private void setInitialView(int view) {
        when(writer.readNumber()).thenReturn((long) view);
    }
}

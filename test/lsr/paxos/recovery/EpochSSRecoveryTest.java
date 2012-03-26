package lsr.paxos.recovery;

import static org.junit.Assert.assertArrayEquals;
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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.BitSet;

import lsr.common.DirectoryHelper;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.CatchUp;
import lsr.paxos.CatchUpListener;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EpochSSRecoveryTest {
    private static final String EPOCH_FILE_NAME = "sync.epoch";
    private String logPath = "bin/logs";
    private RecoveryListener listener;
    private DecideCallback decideCallback;
    private SnapshotProvider snapshotProvider;
    private EpochSSRecovery epochSSRecovery;
    private MockNetwork network;
    private MockDispatcher dispatcher;
    private CatchUp catchUp;

    @Before
    public void setUp() throws IOException {
        Network.removeAllMessageListeners();
        ProcessDescriptorHelper.initialize(3, 0);
        DirectoryHelper.create(logPath);

        snapshotProvider = mock(SnapshotProvider.class);
        decideCallback = mock(DecideCallback.class);
        listener = mock(RecoveryListener.class);
        catchUp = mock(CatchUp.class);

        dispatcher = new MockDispatcher();
        network = mock(MockNetwork.class);
        when(network.fireReceive(any(Message.class), anyInt())).thenCallRealMethod();
    }

    @After
    public void tearDown() {
        DirectoryHelper.delete(logPath);
    }

    @Test
    public void shouldRecoverOnFirstRun() throws IOException {
        initializeRecovery();

        verify(listener).recoveryFinished();

        Paxos paxos = epochSSRecovery.getPaxos();
        assertArrayEquals(new long[] {1, 0, 0}, paxos.getStorage().getEpoch());
    }

    @Test
    public void shouldRespondToRecoveryMessageWhenRecovered() throws IOException {
        initializeRecovery();

        verify(listener).recoveryFinished();

        Recovery recovery = new Recovery(-1, 5);
        network.fireReceive(recovery, 1);
        dispatcher.execute();
        verify(network).sendMessage(any(Message.class), eq(1));
    }

    @Test
    public void shouldRetransmittRecoveryMessageToAll() throws IOException {
        setEpoch(1);
        initializeRecovery();

        // all except local
        BitSet all = new BitSet(3);
        all.set(1, 3);

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(all));
        Recovery recovery = (Recovery) messageArgument.getValue();
        assertEquals(-1, recovery.getView());
        assertEquals(2, recovery.getEpoch());

        dispatcher.advanceTime(3000);
        dispatcher.execute();

        messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(2)).sendMessage(messageArgument.capture(), eq(all));
        recovery = (Recovery) messageArgument.getValue();
        assertEquals(-1, recovery.getView());
        assertEquals(2, recovery.getEpoch());
    }

    @Test
    public void shouldStopTransmittingRecoveryAfterRecevingRecoveryAnswer() throws IOException {
        setEpoch(1);
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
        assertEquals(-1, recovery.getView());
        assertEquals(2, recovery.getEpoch());
    }

    @Test
    public void shouldWaitForMajorityOfRecoveryAnswer() throws IOException {
        setEpoch(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 2, 3}, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 12), 2);
        dispatcher.execute();

        catchUp(12);

        verify(listener).recoveryFinished();

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        assertArrayEquals(new long[] {2, 4, 3}, storage.getEpoch());
    }

    @Test
    public void shouldUpdateLogBeforeCatchUp() throws IOException {
        setEpoch(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 2, 3}, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 12), 2);
        dispatcher.execute();

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        assertEquals(12, storage.getLog().getNextId());
    }

    @Test
    public void shouldForceCatchUp() throws IOException {
        setEpoch(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 2, 3}, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 12), 2);
        dispatcher.execute();

        catchUp(10);
        verify(catchUp).forceCatchup();
        catchUp(12);

        verify(listener).recoveryFinished();

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        assertArrayEquals(new long[] {2, 4, 3}, storage.getEpoch());
    }

    @Test
    public void shouldRecoverWhenNoInstanceWasDecided() throws IOException {
        setEpoch(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 2, 3}, 0), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 0), 2);
        dispatcher.execute();

        verify(catchUp, times(0)).start();
        verify(listener).recoveryFinished();

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        assertEquals(0, storage.getLog().getNextId());
    }

    @Test
    public void shouldWaitForMessageFromLeaderAndRetransmittToAll() throws IOException {
        ProcessDescriptorHelper.initialize(5, 0);
        setEpoch(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(4, new long[] {2, 2, 3, 2, 2}, 0), 1);
        network.fireReceive(new RecoveryAnswer(4, new long[] {2, 4, 2, 2, 2}, 0), 2);
        network.fireReceive(new RecoveryAnswer(4, new long[] {2, 4, 2, 2, 2}, 0), 3);
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
        setEpoch(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(9, new long[] {2, 4, 2, 2, 2}, 0), 2);
        network.fireReceive(new RecoveryAnswer(9, new long[] {2, 4, 2, 2, 2}, 0), 3);
        network.fireReceive(new RecoveryAnswer(6, new long[] {2, 2, 3, 2, 2}, 0), 1);
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
        setEpoch(1);
        initializeRecovery();

        network.fireReceive(new RecoveryAnswer(6, new long[] {2, 2, 3, 2, 2}, 0), 1);
        network.fireReceive(new RecoveryAnswer(9, new long[] {2, 4, 2, 2, 2}, 0), 2);
        network.fireReceive(new RecoveryAnswer(9, new long[] {2, 4, 2, 2, 2}, 0), 3);
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

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        for (int i = 0; i < instanceId; i++) {
            storage.getLog().getInstance(i).setValue(2, new byte[] {1, 2});
            storage.getLog().getInstance(i).setDecided();
        }
        storage.updateFirstUncommitted();
        catchUpListener.getValue().catchUpSucceeded();
    }

    private void initializeRecovery() throws IOException {
        epochSSRecovery = new EpochSSRecovery(snapshotProvider, decideCallback, logPath) {
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
        epochSSRecovery.addRecoveryListener(listener);
        epochSSRecovery.start();
        dispatcher.execute();
    }

    private void setEpoch(long epoch) throws IOException {
        File epochFile = new File(logPath, EPOCH_FILE_NAME + ".0");
        DataOutputStream stream = new DataOutputStream(new FileOutputStream(epochFile));
        stream.writeLong(1);
        stream.close();
    }
}
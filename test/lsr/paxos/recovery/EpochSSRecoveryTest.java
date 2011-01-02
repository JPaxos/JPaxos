package lsr.paxos.recovery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import lsr.common.DirectoryHelper;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.CatchUp;
import lsr.paxos.CatchUpListener;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.MessageHandler;
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
    private MessageHandler recoveryMessageHandler;
    private CatchUp catchUp;

    @Before
    public void setUp() throws IOException {
        ProcessDescriptorHelper.initialize(3, 0);
        DirectoryHelper.create(logPath);

        snapshotProvider = mock(SnapshotProvider.class);
        decideCallback = mock(DecideCallback.class);
        listener = mock(RecoveryListener.class);
        catchUp = mock(CatchUp.class);

        dispatcher = new MockDispatcher();
        network = new MockNetwork();
        recoveryMessageHandler = mock(MessageHandler.class);
    }

    @After
    public void tearDown() {
        DirectoryHelper.delete(logPath);
    }

    @Test
    public void shouldRecoverOnFirstRun() throws IOException {
        initializeRecovery();
        epochSSRecovery.start();

        verify(listener).recoveryFinished();

        Paxos paxos = epochSSRecovery.getPaxos();
        assertArrayEquals(new long[] {1, 0, 0}, paxos.getStorage().getEpoch());
    }

    @Test
    public void shouldWaitForMajorityOfRecoveryAnswer() throws IOException {
        setEpoch(1);
        initializeRecovery();
        epochSSRecovery.start();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 2, 3}, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 12), 2);

        catchUp(12);

        verify(listener).recoveryFinished();

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        assertArrayEquals(new long[] {2, 4, 3}, storage.getEpoch());
    }

    @Test
    public void shouldUpdateLogBeforeCatchUp() throws IOException {
        setEpoch(1);
        initializeRecovery();
        epochSSRecovery.start();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 2, 3}, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 12), 2);
        dispatcher.execute();

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        assertEquals(13, storage.getLog().getNextId());
    }

    @Test
    public void shouldForceCatchUp() throws IOException {
        setEpoch(1);
        initializeRecovery();
        epochSSRecovery.start();

        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 2, 3}, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {2, 4, 2}, 12), 2);

        catchUp(10);
        verify(catchUp).forceCatchup();
        catchUp(12);

        verify(listener).recoveryFinished();

        Storage storage = epochSSRecovery.getPaxos().getStorage();
        assertArrayEquals(new long[] {2, 4, 3}, storage.getEpoch());
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
        epochSSRecovery = new EpochSSRecovery(snapshotProvider, decideCallback, logPath,
                recoveryMessageHandler) {
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
    }

    private void setEpoch(long epoch) throws IOException {
        File epochFile = new File(logPath, EPOCH_FILE_NAME);
        epochFile.createNewFile();
        DataOutputStream stream = new DataOutputStream(new FileOutputStream(epochFile));
        stream.writeLong(1);
        stream.close();
    }
}
package lsr.paxos.recovery;

import static org.junit.Assert.assertArrayEquals;
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
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.storage.PublicDiscWriter;
import lsr.paxos.storage.Storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EpochSSRecoveryTest {
    private String logPath = "bin/logs";
    private RecoveryListener listener;
    private DecideCallback decideCallback;
    private SnapshotProvider snapshotProvider;
    private EpochSSRecovery epochSSRecovery;
    private MockNetwork network;
    private MockDispatcher dispatcher;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        DirectoryHelper.create(logPath);

        snapshotProvider = mock(SnapshotProvider.class);
        decideCallback = mock(DecideCallback.class);
        listener = mock(RecoveryListener.class);

        dispatcher = new MockDispatcher();
        network = new MockNetwork();
        epochSSRecovery = new EpochSSRecovery(snapshotProvider, decideCallback, logPath) {
            protected Paxos createPaxos(DecideCallback decideCallback,
                                        SnapshotProvider snapshotProvider, Storage storage)
                    throws IOException {
                Paxos paxos = mock(Paxos.class);
                when(paxos.getStorage()).thenReturn(storage);
                when(paxos.getNetwork()).thenReturn(network);
                when(paxos.getDispatcher()).thenReturn(dispatcher);
                return paxos;
            }
        };
        epochSSRecovery.addRecoveryListener(listener);
    }

    @After
    public void tearDown() {
        DirectoryHelper.delete(logPath);
    }

    @Test
    public void shouldRecoverOnFirstRun() throws IOException {
        epochSSRecovery.start();

        ArgumentCaptor<Paxos> paxos = ArgumentCaptor.forClass(Paxos.class);
        verify(listener).recoveryFinished(paxos.capture(), any(PublicDiscWriter.class));

        assertArrayEquals(new long[] {1, 0, 0}, paxos.getValue().getStorage().getEpoch());
    }

    @Test
    public void shouldWaitForMajorityOfRecoveryAnswer() throws IOException {
        setEpoch(1);

        epochSSRecovery.start();

        network.fireReceive(new RecoveryAnswer(5, new long[] {0, 2, 3}, 10), 1);
        network.fireReceive(new RecoveryAnswer(5, new long[] {0, 2, 3}, 10), 2);

        ArgumentCaptor<Paxos> paxos = ArgumentCaptor.forClass(Paxos.class);
        verify(listener).recoveryFinished(paxos.capture(), any(PublicDiscWriter.class));

        assertArrayEquals(new long[] {2, 2, 3}, paxos.getValue().getStorage().getEpoch());
    }

    private void setEpoch(long epoch) throws IOException {
        File epochFile = new File(logPath, "sync.epoch");
        epochFile.createNewFile();
        DataOutputStream stream = new DataOutputStream(new FileOutputStream(epochFile));
        stream.writeLong(1);
        stream.close();
    }
}
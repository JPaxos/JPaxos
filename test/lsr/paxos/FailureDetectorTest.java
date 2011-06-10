package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.recovery.MockDispatcher;
import lsr.paxos.recovery.MockNetwork;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;

public class FailureDetectorTest {
    private Paxos paxos;
    private Storage storage;
    private Log log;
    private MockDispatcher dispatcher;
    private MockNetwork network;

    @Before
    public void setUp() {
        dispatcher = new MockDispatcher();
        network = new MockNetwork();
        paxos = mock(Paxos.class);
        storage = mock(Storage.class);
        log = mock(Log.class);

        when(storage.getLog()).thenReturn(log);
        when(paxos.getDispatcher()).thenReturn(dispatcher);

        ProcessDescriptorHelper.initialize(3, 1);
    }

    @Test
    public void shouldSendAliveToAllWhenLeader() throws InterruptedException {
        when(log.getNextId()).thenReturn(100);
        when(storage.getView()).thenReturn(5);
        when(paxos.isLeader()).thenReturn(true);

        PassiveFailureDetector failureDetector = new PassiveFailureDetector(paxos, network, storage);
        failureDetector.start();

        dispatcher.execute();

        failureDetector.stop();

        Message message = network.getMessagesSentToAll().get(0);

        assertTrue(message instanceof Alive);
        Alive alive = (Alive) message;
        assertEquals(5, alive.getView());
        assertEquals(100, alive.getLogSize());
    }

    @Test
    public void shouldRegisterNetworkListenersInStartMethod() throws InterruptedException {
        Message message = new Alive(5, 0);
        when(storage.getView()).thenReturn(5);
        when(paxos.isLeader()).thenReturn(false);
        when(paxos.getLeaderId()).thenReturn(1);

        PassiveFailureDetector failureDetector = new PassiveFailureDetector(paxos, network, storage);
        network.fireReceive(message, 1);
        failureDetector.start();

        failureDetector.stop();
    }
}

package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import lsr.common.Configuration;
import lsr.common.Dispatcher;
import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FailureDetectorTest {

    private class MockNetwork extends Network {
        public void sendMessage(Message message, int destination) {
        }

        public void sendMessage(Message message, BitSet destination) {
        }

        public void sendToAll(Message message) {
        }

        public void fireReceive(Message message, int sender) {
            fireReceiveMessage(message, sender);
        }
    }

    private Paxos paxos;
    private Storage storage;
    private Log log;
    private Dispatcher dispatcher;

    @Before
    public void setUp() {
        dispatcher = new Dispatcher("test");
        dispatcher.start();
        paxos = mock(Paxos.class);
        storage = mock(Storage.class);
        log = mock(Log.class);

        when(storage.getLog()).thenReturn(log);
        when(paxos.getDispatcher()).thenReturn(dispatcher);

        List<PID> processes = new ArrayList<PID>();
        processes.add(mock(PID.class));
        processes.add(mock(PID.class));
        processes.add(mock(PID.class));
        Configuration configuration = new Configuration(processes);
        ProcessDescriptor.initialize(configuration, 0);
    }

    @Test
    public void shouldSendAliveToAllWhenLeader() throws InterruptedException {
        Network network = mock(MockNetwork.class);
        when(log.getNextId()).thenReturn(100);
        when(storage.getView()).thenReturn(5);
        when(paxos.isLeader()).thenReturn(true);

        FailureDetector failureDetector = new FailureDetector(paxos, network, storage);
        failureDetector.start();

        Thread.sleep(100);

        failureDetector.stop();
        dispatcher.interrupt();
        dispatcher.join();

        ArgumentCaptor<Message> message = ArgumentCaptor.forClass(Message.class);
        verify(network).sendToAll(message.capture());

        assertTrue(message.getValue() instanceof Alive);
        Alive alive = (Alive) message.getValue();
        assertEquals(5, alive.getView());
        assertEquals(100, alive.getLogSize());
    }

    @Test
    public void shouldRegisterNetworkListenersInStartMethod() throws InterruptedException {
        Message message = new Alive(5, 0);
        MockNetwork network = new MockNetwork();
        when(storage.getView()).thenReturn(5);
        when(paxos.isLeader()).thenReturn(false);
        when(paxos.getLeaderId()).thenReturn(1);

        FailureDetector failureDetector = new FailureDetector(paxos, network, storage);
        network.fireReceive(message, 1);
        failureDetector.start();

        Thread.sleep(100);
        failureDetector.stop();
        dispatcher.interrupt();
        dispatcher.join();
    }
}

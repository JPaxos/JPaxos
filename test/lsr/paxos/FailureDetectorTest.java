package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import lsr.common.Dispatcher;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FailureDetectorTest {

    @Test
    public void shouldSendAliveToAllWhenLeader() throws InterruptedException {
        Dispatcher dispatcher = new Dispatcher("test");
        dispatcher.start();
        Paxos paxos = mock(Paxos.class);
        Network network = mock(Network.class);
        Storage storage = mock(Storage.class);
        StableStorage stableStorage = mock(StableStorage.class);
        Log log = mock(Log.class);

        when(storage.getLog()).thenReturn(log);
        when(storage.getStableStorage()).thenReturn(stableStorage);
        when(paxos.getDispatcher()).thenReturn(dispatcher);

        when(log.getNextId()).thenReturn(100);
        when(stableStorage.getView()).thenReturn(5);
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
}

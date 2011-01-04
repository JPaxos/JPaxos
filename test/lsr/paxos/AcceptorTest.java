package lsr.paxos;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.BitSet;

import lsr.common.Dispatcher;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AcceptorTest {

    private Dispatcher dispatcher;
    private Paxos paxos;
    private Storage storage;
    private Network network;
    private Acceptor acceptor;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);

        dispatcher = mock(Dispatcher.class);
        when(dispatcher.amIInDispatcher()).thenReturn(true);
        paxos = mock(Paxos.class);
        when(paxos.getDispatcher()).thenReturn(dispatcher);
        storage = new InMemoryStorage();
        network = mock(Network.class);
        acceptor = new Acceptor(paxos, storage, network);
    }

    @Test
    public void shouldHandlePrepareMessage() {
        storage.setView(5);

        Prepare prepare = new Prepare(6, 1);
        acceptor.onPrepare(prepare, 1);

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(1));
        PrepareOK prepareOk = (PrepareOK) messageArgument.getValue();
        assertArrayEquals(new long[] {}, prepareOk.getEpoch());
        assertEquals(6, prepareOk.getView());
        assertArrayEquals(new ConsensusInstance[] {}, prepareOk.getPrepared());
    }

    @Test
    public void shouldAddEpochVectorToPrepareOk() {
        storage.setView(5);
        storage.setEpoch(new long[] {2, 3, 1});

        Prepare prepare = new Prepare(6, 1);
        acceptor.onPrepare(prepare, 1);

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(1));
        PrepareOK prepareOk = (PrepareOK) messageArgument.getValue();
        assertArrayEquals(new long[] {2, 3, 1}, prepareOk.getEpoch());
        assertEquals(6, prepareOk.getView());
        assertArrayEquals(new ConsensusInstance[] {}, prepareOk.getPrepared());
    }

    @Test
    public void shouldSendPreparedInstances() {
        Log log = storage.getLog();
        log.getInstance(4);
        assertEquals(5, log.getNextId());

        Prepare prepare = new Prepare(6, 2);
        acceptor.onPrepare(prepare, 1);

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(1));
        PrepareOK prepareOk = (PrepareOK) messageArgument.getValue();
        assertArrayEquals(
                new ConsensusInstance[] {log.getInstance(2), log.getInstance(3), log.getInstance(4)},
                prepareOk.getPrepared());
    }

    @Test
    public void shouldHandleProposeMessage() {
        storage.setView(1);
        Propose propose = new Propose(1, 0, new byte[] {1, 2, 3});
        acceptor.onPropose(propose, 1);

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), any(BitSet.class));
        Accept accept = (Accept) messageArgument.getValue();
        assertEquals(propose.getView(), accept.getView());
        assertEquals(propose.getInstanceId(), accept.getInstanceId());
    }
}

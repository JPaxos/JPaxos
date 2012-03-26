package lsr.paxos.recovery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import lsr.paxos.Paxos;
import lsr.paxos.Proposer;
import lsr.paxos.Proposer.ProposerState;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.Network;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EpochRecoveryRequestHandlerTest {
    private Paxos paxos;
    private Proposer proposer;
    private MockDispatcher dispatcher;
    private Storage storage;
    private Network network;
    private EpochRecoveryRequestHandler requestHandler;

    @Before
    public void setUp() {
        paxos = mock(Paxos.class);
        proposer = mock(Proposer.class);
        dispatcher = new MockDispatcher();
        storage = new InMemoryStorage();
        network = mock(Network.class);
        requestHandler = new EpochRecoveryRequestHandler(paxos);

        when(paxos.getDispatcher()).thenReturn(dispatcher);
        when(paxos.getProposer()).thenReturn(proposer);
        when(paxos.getStorage()).thenReturn(storage);
        when(paxos.getNetwork()).thenReturn(network);
    }

    // process 0 is sending Recovery<-5> to process 1
    // process 1 - epoch[4, 6, 7], view(8)
    @Test
    public void shouldRespondWhenNotALeader() {
        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        storage.setView(8);
        storage.setEpoch(new long[] {4, 6, 7});
        storage.getLog().getInstance(110);

        Recovery recovery = new Recovery(-1, 5);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(0));
        RecoveryAnswer recoveryAnswer = (RecoveryAnswer) messageArgument.getValue();
        assertArrayEquals(new long[] {5, 6, 7}, recoveryAnswer.getEpoch());
        assertEquals(8, recoveryAnswer.getView());
        assertEquals(111, recoveryAnswer.getNextId());
    }

    // process 0 is sending Recovery<-5> to process 1
    // process 1 - epoch[4, 6, 7], view(8)
    @Test
    public void shouldSendRespondInDispatcherThread() {
        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        storage.setView(8);
        storage.setEpoch(new long[] {4, 6, 7});
        storage.getLog().getInstance(110);

        Recovery recovery = new Recovery(-1, 5);
        requestHandler.onMessageReceived(recovery, 0);

        verify(network, never()).sendMessage(any(Message.class), anyInt());
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(0));
        RecoveryAnswer recoveryAnswer = (RecoveryAnswer) messageArgument.getValue();
        assertArrayEquals(new long[] {5, 6, 7}, recoveryAnswer.getEpoch());
        assertEquals(8, recoveryAnswer.getView());
        assertEquals(111, recoveryAnswer.getNextId());
    }

    // process 0 is sending Recovery<5> to process 1
    // process 1 - epoch[4, 6, 7], view(7)
    @Test
    public void shouldRespondWhenPreparedLeader() {
        when(paxos.getLeaderId()).thenReturn(1);
        when(paxos.isLeader()).thenReturn(true);
        when(proposer.getState()).thenReturn(ProposerState.PREPARED);
        storage.setView(7);
        storage.setEpoch(new long[] {4, 6, 7});
        storage.getLog().getInstance(110);

        Recovery recovery = new Recovery(-1, 5);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(0));
        RecoveryAnswer recoveryAnswer = (RecoveryAnswer) messageArgument.getValue();
        assertArrayEquals(new long[] {5, 6, 7}, recoveryAnswer.getEpoch());
        assertEquals(7, recoveryAnswer.getView());
        assertEquals(111, recoveryAnswer.getNextId());
    }

    // process 0 is sending Recovery<7> to process 1
    // process 1 - view(6)
    @Test
    public void shouldNotRespondWhenSenderIsLeader() {
        when(paxos.getLeaderId()).thenReturn(0);
        when(paxos.isLeader()).thenReturn(false);
        storage.setView(6);

        Recovery recovery = new Recovery(-1, 7);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    // process 0 is sending Recovery<7> to process 1
    // process 1 - view(7)
    @Test
    public void shouldNotRespondWhenNotPreparedLeader() {
        when(paxos.getLeaderId()).thenReturn(1);
        when(paxos.isLeader()).thenReturn(true);
        when(proposer.getState()).thenReturn(ProposerState.PREPARING);
        storage.setView(7);

        Recovery recovery = new Recovery(-1, 5);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    // process 0 is sending Recovery<3> to process 1
    // process 1 - epoch[4,6,7], view(8)
    @Test
    public void shouldDiscardOldRecoveryMessages() {
        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        storage.setView(8);
        storage.setEpoch(new long[] {4, 6, 7});

        Recovery recovery = new Recovery(-1, 3);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    @Test(expected = ClassCastException.class)
    public void shouldThrowExceptionWhenReceivedNotRecoveryMessage() {
        Alive alive = new Alive(3, 4);
        requestHandler.onMessageReceived(alive, 0);
    }
}

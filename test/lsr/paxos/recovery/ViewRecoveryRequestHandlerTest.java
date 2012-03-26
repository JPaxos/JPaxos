package lsr.paxos.recovery;

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
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ViewRecoveryRequestHandlerTest {
    private Paxos paxos;
    private Proposer proposer;
    private MockDispatcher dispatcher;
    private Log log;
    private Storage storage;
    private Network network;
    private ViewRecoveryRequestHandler requestHandler;

    @Before
    public void setUp() {
        paxos = mock(Paxos.class);
        proposer = mock(Proposer.class);
        dispatcher = new MockDispatcher();
        log = mock(Log.class);
        storage = mock(Storage.class);
        network = mock(Network.class);
        requestHandler = new ViewRecoveryRequestHandler(paxos);

        when(paxos.getDispatcher()).thenReturn(dispatcher);
        when(paxos.getProposer()).thenReturn(proposer);
        when(paxos.getStorage()).thenReturn(storage);
        when(paxos.getNetwork()).thenReturn(network);
        when(storage.getLog()).thenReturn(log);
    }

    // process 0 is sending Recovery<5> to process 1
    // process 1 is in view 8
    @Test
    public void shouldRespondWhenNotALeader() {
        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        when(storage.getView()).thenReturn(8);
        when(log.getNextId()).thenReturn(111);

        Recovery recovery = new Recovery(5, -1);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(0));
        RecoveryAnswer recoveryAnswer = (RecoveryAnswer) messageArgument.getValue();
        assertEquals(8, recoveryAnswer.getView());
        assertEquals(111, recoveryAnswer.getNextId());
    }

    // process 0 is sending Recovery<5> to process 1
    // process 1 is in view 7
    @Test
    public void shouldRespondWhenPreparedLeader() {
        when(paxos.getLeaderId()).thenReturn(1);
        when(paxos.isLeader()).thenReturn(true);
        when(storage.getView()).thenReturn(7);
        when(log.getNextId()).thenReturn(111);
        when(proposer.getState()).thenReturn(ProposerState.PREPARED);

        Recovery recovery = new Recovery(5, -1);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), eq(0));
        RecoveryAnswer recoveryAnswer = (RecoveryAnswer) messageArgument.getValue();
        assertEquals(7, recoveryAnswer.getView());
        assertEquals(111, recoveryAnswer.getNextId());
    }

    // process 0 is sending Recovery<7> to process 1
    // process 1 is in view 6
    @Test
    public void shouldNotRespondWhenSenderIsLeader() {
        when(paxos.getLeaderId()).thenReturn(0);
        when(paxos.isLeader()).thenReturn(false);
        when(storage.getView()).thenReturn(6);

        Recovery recovery = new Recovery(7, -1);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    // process 0 is sending Recovery<7> to process 1
    // process 1 is in view 7
    @Test
    public void shouldNotRespondWhenNotPreparedLeader() {
        when(paxos.getLeaderId()).thenReturn(1);
        when(paxos.isLeader()).thenReturn(true);
        when(storage.getView()).thenReturn(7);
        when(proposer.getState()).thenReturn(ProposerState.PREPARING);

        Recovery recovery = new Recovery(5, -1);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }

    // process 0 is sending Recovery<10> to process 1
    // process 1 is in view 8
    @Test
    public void shouldAdvanceTheView() {
        when(paxos.getLeaderId()).thenReturn(2);
        when(paxos.isLeader()).thenReturn(false);
        when(storage.getView()).thenReturn(8);

        Recovery recovery = new Recovery(10, -1);
        requestHandler.onMessageReceived(recovery, 0);
        dispatcher.execute();

        verify(paxos).advanceView(10);
        verify(network, never()).sendMessage(any(Message.class), anyInt());
    }
}

package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Accept;
import lsr.paxos.recovery.MockDispatcher;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;

public class LearnerTest {
    private MockDispatcher dispatcher;
    private Paxos paxos;
    private Proposer proposer;
    private Storage storage;
    private Learner learner;
    private Log log;

    @Before
    public void setUp() {
        Logger.getLogger(Learner.class.getCanonicalName()).setLevel(Level.ALL);
        ProcessDescriptorHelper.initialize(3, 0);
        dispatcher = new MockDispatcher();
        dispatcher.forceBeingInDispatcher();
        paxos = mock(Paxos.class);
        when(paxos.getDispatcher()).thenReturn(dispatcher);
        proposer = mock(Proposer.class);
        log = mock(Log.class);
        storage = mock(Storage.class);
        when(storage.getLog()).thenReturn(log);
        learner = new Learner(paxos, proposer, storage);
        when(storage.getView()).thenReturn(5);
    }

    @Test
    public void shouldInformProposerAboutNewAcceptIfLeader() {
        when(log.getInstance(10)).thenReturn(new ConsensusInstance(10));
        when(paxos.isLeader()).thenReturn(true);

        Accept accept = new Accept(5, 10);
        learner.onAccept(accept, 1);

        verify(proposer).stopPropose(10, 1);
    }

    @Test
    public void shouldNotInformProposerAboutNewAcceptIfLeader() {
        when(log.getInstance(10)).thenReturn(new ConsensusInstance(10));
        when(paxos.isLeader()).thenReturn(false);

        Accept accept = new Accept(5, 10);
        learner.onAccept(accept, 1);

        verifyZeroInteractions(proposer);
    }

    @Test
    public void shouldDecideIfReceivedAcceptFromMajority() {
        when(log.getInstance(10)).thenReturn(new ConsensusInstance(10));
        log.getInstance(10).getAccepts().set(2);
        log.getInstance(10).setValue(5, new byte[] {1, 2, 3});
        when(paxos.isLeader()).thenReturn(false);

        Accept accept = new Accept(5, 10);
        learner.onAccept(accept, 1);

        verifyZeroInteractions(proposer);
        verify(paxos).decide(10);
    }

    @Test
    public void shouldNotDecideIfInstanceValueIsUnknown() {
        when(log.getInstance(10)).thenReturn(new ConsensusInstance(10));
        log.getInstance(10).getAccepts().set(2);
        log.getInstance(10).setValue(5, null);
        when(paxos.isLeader()).thenReturn(false);

        Accept accept = new Accept(5, 10);
        learner.onAccept(accept, 1);

        verifyZeroInteractions(proposer);
        verify(paxos, never()).decide(10);
    }

    @Test
    public void shouldIgnoreAcceptIfInstanceAlreadyDecided() {
        when(log.getInstance(10)).thenReturn(new ConsensusInstance(10));
        log.getInstance(10).getAccepts().set(2);
        log.getInstance(10).setValue(5, new byte[] {1, 2, 3});
        log.getInstance(10).setDecided();
        when(paxos.isLeader()).thenReturn(true);

        Accept accept = new Accept(5, 10);
        learner.onAccept(accept, 1);

        verifyZeroInteractions(proposer);
        verify(paxos, never()).decide(10);
    }

    @Test
    public void shouldUpdateInstanceValueIfNewerView() {
        when(log.getInstance(10)).thenReturn(new ConsensusInstance(10));
        log.getInstance(10).getAccepts().set(2);
        log.getInstance(10).setValue(3, new byte[] {1, 2, 3});
        when(paxos.isLeader()).thenReturn(false);

        Accept accept = new Accept(5, 10);
        learner.onAccept(accept, 1);

        assertEquals(1, log.getInstance(10).getAccepts().cardinality());
        assertEquals(true, log.getInstance(10).getAccepts().get(1));
        assertEquals(null, log.getInstance(10).getValue());
        assertEquals(5, log.getInstance(10).getView());
        verifyZeroInteractions(proposer);
        verify(paxos, never()).decide(10);
    }

    @Test
    public void shouldIgnoreAcceptForTooOldInstances() {
        when(log.getInstance(10)).thenReturn(null);

        Accept accept = new Accept(5, 10);
        learner.onAccept(accept, 1);

        verifyZeroInteractions(proposer);
        verify(paxos, never()).decide(10);
    }
}

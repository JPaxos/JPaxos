package lsr.paxos;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.BitSet;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;

public class EpochPrepareRetransmitterTest {
    private Retransmitter retransmitter;
    private Prepare prepare;
    private BitSet acceptors;
    private PrepareRetransmitter prepareRetransmitter;
    private Storage storage;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        retransmitter = mock(Retransmitter.class);
        storage = new InMemoryStorage();
        prepare = new Prepare(3, 10);
        acceptors = new BitSet();

        storage.setEpoch(new long[] {0, 0, 0});

        prepareRetransmitter = new EpochPrepareRetransmitter(retransmitter, storage);
    }

    @Test
    public void shouldStartTransmittingAMessage() {
        prepareRetransmitter.startTransmitting(prepare, acceptors);

        verify(retransmitter).startTransmitting(prepare, acceptors);
    }

    @Test
    public void shouldUpdateEpoch() {
        RetransmittedMessage retransmittedMessage = mock(RetransmittedMessage.class);
        when(retransmitter.startTransmitting(prepare, acceptors)).thenReturn(retransmittedMessage);
        prepareRetransmitter.startTransmitting(prepare, acceptors);

        PrepareOK prepareOk1 = new PrepareOK(1, new ConsensusInstance[] {}, new long[] {1, 3, 2});
        prepareRetransmitter.update(prepareOk1, 1);
        assertArrayEquals(new long[] {1, 3, 2}, storage.getEpoch());
    }

    @Test
    public void shouldStopTransmittingAfterReceivingPrepareOk() {
        RetransmittedMessage retransmittedMessage = mock(RetransmittedMessage.class);
        when(retransmitter.startTransmitting(prepare, acceptors)).thenReturn(retransmittedMessage);
        prepareRetransmitter.startTransmitting(prepare, acceptors);

        assertFalse(prepareRetransmitter.isMajority());
        PrepareOK prepareOk1 = new PrepareOK(1, new ConsensusInstance[] {}, new long[] {1, 1, 1});
        prepareRetransmitter.update(prepareOk1, 1);
        verify(retransmittedMessage).stop(1);

        assertFalse(prepareRetransmitter.isMajority());
        PrepareOK prepareOk2 = new PrepareOK(1, new ConsensusInstance[] {}, new long[] {1, 1, 1});
        prepareRetransmitter.update(prepareOk2, 2);
        verify(retransmittedMessage).stop(2);

        assertTrue(prepareRetransmitter.isMajority());
    }

    @Test
    public void shouldDiscardResponsesWithOldEpoch() {
        RetransmittedMessage retransmittedMessage = mock(RetransmittedMessage.class);
        when(retransmitter.startTransmitting(prepare, acceptors)).thenReturn(retransmittedMessage);
        prepareRetransmitter.startTransmitting(prepare, acceptors);
        assertFalse(prepareRetransmitter.isMajority());

        PrepareOK prepareOk1 = new PrepareOK(1, new ConsensusInstance[] {}, new long[] {1, 1, 1});
        prepareRetransmitter.update(prepareOk1, 1);
        verify(retransmittedMessage).stop(1);
        assertFalse(prepareRetransmitter.isMajority());

        PrepareOK prepareOk2 = new PrepareOK(1, new ConsensusInstance[] {}, new long[] {1, 2, 1});
        prepareRetransmitter.update(prepareOk2, 2);
        verify(retransmittedMessage).stop(2);
        verify(retransmittedMessage).start(1);
        assertFalse(prepareRetransmitter.isMajority());

        PrepareOK prepareOk3 = new PrepareOK(1, new ConsensusInstance[] {}, new long[] {1, 2, 1});
        prepareRetransmitter.update(prepareOk3, 1);
        verify(retransmittedMessage, times(2)).stop(2);
        assertTrue(prepareRetransmitter.isMajority());

        verify(retransmittedMessage, never()).stop();
    }

    @Test
    public void shouldStopTransmitting() {
        RetransmittedMessage retransmittedMessage = mock(RetransmittedMessage.class);
        when(retransmitter.startTransmitting(prepare, acceptors)).thenReturn(retransmittedMessage);
        prepareRetransmitter.startTransmitting(prepare, acceptors);

        prepareRetransmitter.stop();

        verify(retransmittedMessage).stop();
    }
}

package lsr.paxos;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.BitSet;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Prepare;

import org.junit.Before;
import org.junit.Test;

public class PrepareRetransmitterImplTest {
    private Retransmitter retransmitter;
    private PrepareRetransmitter prepareRetransmitter;
    private Prepare prepare;
    private BitSet acceptors;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        retransmitter = mock(Retransmitter.class);
        prepareRetransmitter = new PrepareRetransmitterImpl(retransmitter);
        prepare = new Prepare(3, 10);
        acceptors = new BitSet();
    }

    @Test
    public void shouldStartTransmittingAMessage() {
        prepareRetransmitter.startTransmitting(prepare, acceptors);

        verify(retransmitter).startTransmitting(prepare, acceptors);
    }

    @Test
    public void shouldStopTransmittingAfterReceivingPrepareOk() {
        RetransmittedMessage retransmittedMessage = mock(RetransmittedMessage.class);
        when(retransmitter.startTransmitting(prepare, acceptors)).thenReturn(retransmittedMessage);
        prepareRetransmitter.startTransmitting(prepare, acceptors);

        assertFalse(prepareRetransmitter.isMajority());
        prepareRetransmitter.update(null, 1);
        verify(retransmittedMessage).stop(1);

        assertFalse(prepareRetransmitter.isMajority());
        prepareRetransmitter.update(null, 2);
        verify(retransmittedMessage).stop(2);

        assertTrue(prepareRetransmitter.isMajority());
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

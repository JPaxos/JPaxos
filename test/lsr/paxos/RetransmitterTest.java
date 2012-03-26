package lsr.paxos;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.BitSet;

import lsr.common.ProcessDescriptor;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;
import lsr.paxos.recovery.MockDispatcher;

import org.junit.Before;
import org.junit.Test;

public class RetransmitterTest {
    private static final int RETRANSMIT_TIMEOUT = 10000;
    private static final int N_PROCESSES = 3;
    private Retransmitter retransmitter;
    private Network network;
    private MockDispatcher dispatcher;
    private Message message1;
    private Message message2;
    private Message message3;
    private BitSet all;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        all = new BitSet();
        all.set(0, N_PROCESSES);
        network = mock(Network.class);

        dispatcher = new MockDispatcher();
        retransmitter = new Retransmitter(network, N_PROCESSES, dispatcher);

        message1 = mock(Message.class);
        message2 = mock(Message.class);
        message3 = mock(Message.class);
    }

    @Test
    public void shouldSendMessageToAllExceptItself() {
        retransmitter.startTransmitting(message1);

        all.clear(ProcessDescriptor.getInstance().localId);
        verify(network, times(1)).sendMessage(message1, all);
    }

    @Test
    public void shouldSendMessageToSpecifiedDestinations() {
        BitSet destinations = new BitSet();
        destinations.set(0, 2);

        retransmitter.startTransmitting(message1, destinations);

        verify(network, times(1)).sendMessage(message1, destinations);
    }

    @Test
    public void shouldRetransmitMessageAfterTimeout() {
        retransmitter.startTransmitting(message1, all);
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        verify(network, times(2)).sendMessage(message1, all);
    }

    @Test
    public void shouldStopAll() {
        retransmitter.startTransmitting(message1, all);
        retransmitter.stopAll();
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        verify(network, times(1)).sendMessage(message1, all);
    }

    @Test
    public void shouldAllowToRetransmitMultipleMessages() {
        // start transmitting 3 messages
        retransmitter.startTransmitting(message1, all);
        retransmitter.startTransmitting(message2, all);
        retransmitter.startTransmitting(message3, all);

        verify(network, times(1)).sendMessage(message1, all);
        verify(network, times(1)).sendMessage(message2, all);
        verify(network, times(1)).sendMessage(message3, all);

        // simulate waiting RETRANSMIT_TIMEOUT ms
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        // all messages are send twice: at the beginning and after timeout
        verify(network, times(2)).sendMessage(message1, all);
        verify(network, times(2)).sendMessage(message2, all);
        verify(network, times(2)).sendMessage(message3, all);
    }

    @Test
    public void shouldStopRetransmittingSingleMessage() {
        // start transmitting 3 messages
        retransmitter.startTransmitting(message1, all);
        RetransmittedMessage handler = retransmitter.startTransmitting(message2, all);
        retransmitter.startTransmitting(message3, all);

        verify(network, times(1)).sendMessage(message1, all);
        verify(network, times(1)).sendMessage(message2, all);
        verify(network, times(1)).sendMessage(message3, all);

        // stop retransmitting message 2
        handler.stop();

        // simulate waiting RETRANSMIT_TIMEOUT ms
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        // message 2 shouldn't be retransmitted
        verify(network, times(2)).sendMessage(message1, all);
        verify(network, times(1)).sendMessage(message2, all);
        verify(network, times(2)).sendMessage(message3, all);
    }

    @Test
    public void shouldStopRetransmittingToSpecifiedDestination() {
        // start transmitting message
        RetransmittedMessage handler = retransmitter.startTransmitting(message1, all);
        verify(network, times(1)).sendMessage(message1, all);
        reset(network);

        // stop retransmitting to replica with id = 1
        handler.stop(1);

        // simulate waiting RETRANSMIT_TIMEOUT ms
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        all.clear(1);
        verify(network, times(1)).sendMessage(message1, all);
    }

    @Test
    public void shouldStartTransmittingMessageAgain() {
        // start transmitting message
        RetransmittedMessage handler = retransmitter.startTransmitting(message1, all);
        verify(network, times(1)).sendMessage(message1, all);
        reset(network);

        // stop retransmitting to replica with id = 1
        handler.stop(1);

        // simulate waiting RETRANSMIT_TIMEOUT ms
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        all.clear(1);
        verify(network, times(1)).sendMessage(message1, all);

        handler.start(1);

        // simulate waiting RETRANSMIT_TIMEOUT ms
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        all.set(1);
        verify(network, times(2)).sendMessage(message1, all);
    }

    @Test
    public void shouldNotStartRetransmissionAgain() {
        // start transmitting message
        RetransmittedMessage handler = retransmitter.startTransmitting(message1, all);
        verify(network, times(1)).sendMessage(message1, all);
        reset(network);

        // stop retransmitting to replica with id = 1
        handler.stop();

        // simulate waiting RETRANSMIT_TIMEOUT ms
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        handler.start(1);

        // simulate waiting RETRANSMIT_TIMEOUT ms
        dispatcher.advanceTime(RETRANSMIT_TIMEOUT);
        dispatcher.execute();

        verify(network, times(0)).sendMessage(message1, all);
    }
}

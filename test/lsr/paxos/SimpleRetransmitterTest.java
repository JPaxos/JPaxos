package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.BitSet;
import java.util.Timer;
import java.util.TimerTask;

import lsr.common.Dispatcher;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SimpleRetransmitterTest {
    private MockRetransmitter retransmitter;
    private Network network;
    private TimerTask task;
    private Timer timer;
    private Dispatcher dispatcher;
    private static final int N_PROCESSES = 3;
    private Message message1;
    private Message message2;
    private Message message3;

    @Before
    public void setUp() {
        network = mock(Network.class);

        task = mock(TimerTask.class);
        timer = mock(Timer.class);
        dispatcher = mock(Dispatcher.class);
        when(dispatcher.amIInDispatcher()).thenReturn(true);

        retransmitter = new MockRetransmitter(network, N_PROCESSES, dispatcher, timer);

        message1 = mock(Message.class);
        message2 = mock(Message.class);
        message3 = mock(Message.class);
    }

    /**
     * Checks whether the current timer task is stopped when stopAll method is
     * called.
     */
    public void testStopAll() {
        retransmitter.setTimerTask(task);
        retransmitter.stopAll();
        verify(task, times(1)).cancel();
    }

    @Test
    public void testStartRetransmittingToAllSendImmediately() {
        BitSet destinations = new BitSet();
        destinations.set(0, N_PROCESSES);

        retransmitter.startTransmitting(message1);

        ArgumentCaptor<BitSet> destinationArgument = ArgumentCaptor.forClass(BitSet.class);
        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(1)).sendMessage(messageArgument.capture(),
                destinationArgument.capture());

        assertEquals(message1, messageArgument.getValue());
        assertEquals(destinations, destinationArgument.getValue());
    }

    @Test
    public void testStartRetransmittingToDestinationsSendImmediately() {
        BitSet destinations = new BitSet();
        destinations.set(0, 2);

        retransmitter.startTransmitting(message1, destinations);

        ArgumentCaptor<BitSet> destinationArgument = ArgumentCaptor.forClass(BitSet.class);
        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(1)).sendMessage(messageArgument.capture(),
                destinationArgument.capture());
        assertEquals(message1, messageArgument.getValue());
        assertEquals(destinations, destinationArgument.getValue());
    }

    public void testStartRetransmittingStartTimerWhenAddingFirstMessage() {
        retransmitter.startTransmitting(message1);
        verify(timer).scheduleAtFixedRate((TimerTask) any(), anyLong(), anyLong());
    }

    public void testTimerTaskIteratesThroughMessages() {
        BitSet all = new BitSet();
        all.set(0, N_PROCESSES);

        // start transmitting 3 messages
        retransmitter.startTransmitting(message1);
        retransmitter.startTransmitting(message2);
        retransmitter.startTransmitting(message3);

        verify(network, times(1)).sendMessage(message1, all);
        verify(network, times(1)).sendMessage(message2, all);
        verify(network, times(1)).sendMessage(message1, all);

        // simulate running timer task
        retransmitter.getTimerTask().run();

        // all messages are send twice: at the beginning and after executing
        // timer
        verify(network, times(2)).sendMessage(message1, all);
        verify(network, times(2)).sendMessage(message2, all);
        verify(network, times(2)).sendMessage(message3, all);
    }

    public void testStopMessage() {
        BitSet all = new BitSet();
        all.set(0, N_PROCESSES);

        // start transmitting 3 messages
        retransmitter.startTransmitting(message1);
        RetransmittedMessage handler = retransmitter.startTransmitting(message2);
        retransmitter.startTransmitting(message3);

        verify(network, times(1)).sendMessage(message1, all);
        verify(network, times(1)).sendMessage(message2, all);
        verify(network, times(1)).sendMessage(message3, all);

        // stop retransmitting message 2
        handler.stop();

        // simulate running timer task
        retransmitter.getTimerTask().run();

        // message 2 cannot be retransmitted
        verify(network, times(2)).sendMessage(message1, all);
        verify(network, times(1)).sendMessage(message2, all);
        verify(network, times(2)).sendMessage(message3, all);
    }

    /**
     * Checks {@link NullPointerException} on _task field.
     */
    @Test
    public void testStopAllWhenEmpty() {
        retransmitter.stopAll();
    }

    @Test
    public void testStopDestination() {
        BitSet destinations = new BitSet();
        destinations.set(0, N_PROCESSES);
        destinations.clear(1);

        // start transmitting message
        RetransmittedMessage handler = retransmitter.startTransmitting(message1);

        // stop retransmitting to replica with id = 1
        handler.stop(1);

        // simulate running timer task
        retransmitter.getTimerTask().run();

        verify(network, times(1)).sendMessage(message1, destinations);
    }

    public void testClearMessagesAfterStopAll() {
        BitSet all = new BitSet();
        all.set(0, N_PROCESSES);

        retransmitter.startTransmitting(message1);
        retransmitter.stopAll();

        retransmitter.startTransmitting(message2);

        retransmitter.getTimerTask().run();

        // one sending after startTransmiting
        verify(network, times(1)).sendMessage(message1, all);
        // two sending: after startTransmitting and TimerTask.run();
        verify(network, times(2)).sendMessage(message2, all);
    }

    private class MockRetransmitter extends Retransmitter {
        public MockRetransmitter(Network network, int nProcesses, Dispatcher dispatcher, Timer timer) {
            super(network, nProcesses, dispatcher);
            // timer = timer;
        }

        public TimerTask getTimerTask() {
            return task;
        }

        public void setTimerTask(TimerTask task) {
            // task = task;
        }
    }
}

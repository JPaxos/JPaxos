package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
	private MockRetransmitter _retransmitter;
	private Network _network;
	private TimerTask _task;
	private Timer _timer;
	private Dispatcher _dispatcher;
	private static final int N_PROCESSES = 3;
	private Message _message1;
	private Message _message2;
	private Message _message3;

	@Before
	public void setUp() {
		_network = mock(Network.class);

		_task = mock(TimerTask.class);
		_timer = mock(Timer.class);
		_dispatcher = mock(Dispatcher.class);

		_retransmitter = new MockRetransmitter(_network, N_PROCESSES, _dispatcher, _timer);

		_message1 = mock(Message.class);
		_message2 = mock(Message.class);
		_message3 = mock(Message.class);
	}

	/**
	 * Checks whether the current timer task is stopped when stopAll method is
	 * called.
	 */
	public void testStopAll() {
		_retransmitter.setTimerTask(_task);
		_retransmitter.stopAll();
		verify(_task, times(1)).cancel();
	}

	@Test
	public void testStartRetransmittingToAllSendImmediately() {
		BitSet destinations = new BitSet();
		destinations.set(0, N_PROCESSES);

		_retransmitter.startTransmitting(_message1);

		ArgumentCaptor<BitSet> destinationArgument = ArgumentCaptor.forClass(BitSet.class);
		ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
		verify(_network, times(1)).sendMessage(messageArgument.capture(), destinationArgument.capture());
	
		assertEquals(_message1, messageArgument.getValue());
		assertEquals(destinations, destinationArgument.getValue());
	}

	@Test
	public void testStartRetransmittingToDestinationsSendImmediately() {
		BitSet destinations = new BitSet();
		destinations.set(0, 2);

		_retransmitter.startTransmitting(_message1, destinations);

		ArgumentCaptor<BitSet> destinationArgument = ArgumentCaptor.forClass(BitSet.class);
		ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
		verify(_network, times(1)).sendMessage(messageArgument.capture(), destinationArgument.capture());
		assertEquals(_message1, messageArgument.getValue());
		assertEquals(destinations, destinationArgument.getValue());
	}

	public void testStartRetransmittingStartTimerWhenAddingFirstMessage() {
		_retransmitter.startTransmitting(_message1);
		verify(_timer).scheduleAtFixedRate((TimerTask) any(), anyLong(), anyLong());
	}

	public void testTimerTaskIteratesThroughMessages() {
		BitSet all = new BitSet();
		all.set(0, N_PROCESSES);

		// start transmitting 3 messages
		_retransmitter.startTransmitting(_message1);
		_retransmitter.startTransmitting(_message2);
		_retransmitter.startTransmitting(_message3);

		verify(_network, times(1)).sendMessage(_message1, all);
		verify(_network, times(1)).sendMessage(_message2, all);
		verify(_network, times(1)).sendMessage(_message1, all);

		// simulate running timer task
		_retransmitter.getTimerTask().run();

		// all messages are send twice: at the beginning and after executing
		// timer
		verify(_network, times(2)).sendMessage(_message1, all);
		verify(_network, times(2)).sendMessage(_message2, all);
		verify(_network, times(2)).sendMessage(_message3, all);
	}

	public void testStopMessage() {
		BitSet all = new BitSet();
		all.set(0, N_PROCESSES);

		// start transmitting 3 messages
		_retransmitter.startTransmitting(_message1);
		RetransmittedMessage handler = _retransmitter.startTransmitting(_message2);
		_retransmitter.startTransmitting(_message3);

		verify(_network, times(1)).sendMessage(_message1, all);
		verify(_network, times(1)).sendMessage(_message2, all);
		verify(_network, times(1)).sendMessage(_message3, all);

		// stop retransmitting message 2
		handler.stop();

		// simulate running timer task
		_retransmitter.getTimerTask().run();

		// message 2 cannot be retransmitted
		verify(_network, times(2)).sendMessage(_message1, all);
		verify(_network, times(1)).sendMessage(_message2, all);
		verify(_network, times(2)).sendMessage(_message3, all);
	}

	/**
	 * Checks {@link NullPointerException} on _task field.
	 */
	@Test
	public void testStopAllWhenEmpty() {
		_retransmitter.stopAll();
	}

	@Test
	public void testStopDestination() {
		BitSet destinations = new BitSet();
		destinations.set(0, N_PROCESSES);
		destinations.clear(1);

		// start transmitting message
		RetransmittedMessage handler = _retransmitter.startTransmitting(_message1);

		// stop retransmitting to replica with id = 1
		handler.stop(1);

		// simulate running timer task
		_retransmitter.getTimerTask().run();

		verify(_network, times(1)).sendMessage(_message1, destinations);
	}

	public void testClearMessagesAfterStopAll() {
		BitSet all = new BitSet();
		all.set(0, N_PROCESSES);

		_retransmitter.startTransmitting(_message1);
		_retransmitter.stopAll();

		_retransmitter.startTransmitting(_message2);

		_retransmitter.getTimerTask().run();

		// one sending after startTransmiting
		verify(_network, times(1)).sendMessage(_message1, all);
		// two sending: after startTransmitting and TimerTask.run();
		verify(_network, times(2)).sendMessage(_message2, all);
	}

	private class MockRetransmitter extends Retransmitter {
		public MockRetransmitter(
				Network network, 
				int nProcesses, 
				Dispatcher dispatcher, 
				Timer timer) {
			super(network, nProcesses, dispatcher);
			_timer = timer;
		}

		public TimerTask getTimerTask() {
			return _task;
		}

		public void setTimerTask(TimerTask task) {
			_task = task;
		}
	}
}

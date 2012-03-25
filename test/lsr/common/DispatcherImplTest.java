package lsr.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import lsr.common.Dispatcher.Priority;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class DispatcherImplTest {
    private DispatcherImpl dispatcher;

    @Before
    public void setUp() {
        dispatcher = new DispatcherImpl("test");
        dispatcher.start();
    }

    public void tearDown() throws InterruptedException {
        dispatcher.interrupt();
        dispatcher.join();
    }

    @Test
    public void shouldDispatchTask() throws InterruptedException {
        Runnable task = mock(Runnable.class);

        dispatcher.dispatch(task);
        Thread.sleep(100);

        verify(task, times(1)).run();
    }

    @Test
    public void shouldRunHighPriorityTaskFirst() throws InterruptedException {
        Runnable lowPriorityTask = mock(Runnable.class);
        Runnable highPriorityTask = mock(Runnable.class);

        dispatcher.dispatch(new SlowTask(100));
        dispatcher.dispatch(lowPriorityTask, Priority.Low);
        dispatcher.dispatch(highPriorityTask, Priority.High);

        Thread.sleep(150);

        InOrder inOrder = inOrder(lowPriorityTask, highPriorityTask);
        inOrder.verify(highPriorityTask).run();
        inOrder.verify(lowPriorityTask).run();
    }

    @Test
    public void shouldNotBeInDispatcherThread() {
        assertFalse(dispatcher.amIInDispatcher());
    }

    @Test
    public void shouldShowContentInToStringMethod() throws InterruptedException {
        Runnable task = mock(Runnable.class);
        dispatcher.dispatch(new SlowTask(100), Priority.High);
        dispatcher.dispatch(task, Priority.High);
        dispatcher.dispatch(task, Priority.High);
        dispatcher.dispatch(task, Priority.High);
        dispatcher.dispatch(task, Priority.Normal);
        dispatcher.dispatch(task, Priority.Normal);
        dispatcher.dispatch(task, Priority.Low);

        String state = dispatcher.toString();
        assertEquals("High:3,Normal:2,Low:1", state);

        Thread.sleep(150);
    }

    @Test
    public void shouldScheduleTask() throws InterruptedException {
        Runnable firstTask = mock(Runnable.class);
        Runnable secondTask = mock(Runnable.class);

        dispatcher.schedule(secondTask, Priority.Normal, 100);
        dispatcher.schedule(firstTask, Priority.Normal, 50);

        Thread.sleep(150);

        InOrder inOrder = inOrder(firstTask, secondTask);
        inOrder.verify(firstTask).run();
        inOrder.verify(secondTask).run();
    }

    @Test
    public void shouldAllowToCancelScheduledTask() throws InterruptedException {
        Runnable task = mock(Runnable.class);

        PriorityTask priorityTask = dispatcher.schedule(task, Priority.Normal, 100);
        assertTrue(100 - priorityTask.getDelay() < 5);
        priorityTask.cancel();

        Thread.sleep(150);

        verifyZeroInteractions(task);
    }

    @Test
    public void shouldAllowToCancelNormalTask() throws InterruptedException {
        Runnable task = mock(Runnable.class);

        dispatcher.dispatch(new SlowTask(100));
        PriorityTask priorityTask = dispatcher.dispatch(task);
        assertEquals(0, priorityTask.getDelay());
        priorityTask.cancel();

        Thread.sleep(150);
    }

    @Test
    public void shouldScheduleAtFixedRate() throws InterruptedException {
        Runnable firstTask = mock(Runnable.class);
        Runnable secondTask = mock(Runnable.class);

        dispatcher.scheduleAtFixedRate(secondTask, Priority.Normal, 5, 75);
        dispatcher.scheduleAtFixedRate(firstTask, Priority.Normal, 0, 50);

        Thread.sleep(125);

        InOrder inOrder = inOrder(firstTask, secondTask);
        inOrder.verify(firstTask).run();
        inOrder.verify(secondTask).run();
        inOrder.verify(firstTask).run();
        inOrder.verify(secondTask).run();
        inOrder.verify(firstTask).run();

        verifyNoMoreInteractions(firstTask);
        verifyNoMoreInteractions(secondTask);
    }

    @Test
    public void shouldScheduleWithFixedDelay() throws InterruptedException {
        Runnable firstTask = mock(Runnable.class);
        Runnable secondTask = mock(Runnable.class);

        dispatcher.scheduleWithFixedDelay(secondTask, Priority.Normal, 5, 75);
        dispatcher.scheduleWithFixedDelay(firstTask, Priority.Normal, 0, 50);

        Thread.sleep(125);

        InOrder inOrder = inOrder(firstTask, secondTask);
        inOrder.verify(firstTask).run();
        inOrder.verify(secondTask).run();
        inOrder.verify(firstTask).run();
        inOrder.verify(secondTask).run();
        inOrder.verify(firstTask).run();

        verifyNoMoreInteractions(firstTask);
        verifyNoMoreInteractions(secondTask);
    }

    private static class SlowTask implements Runnable {
        private final int sleep;

        public SlowTask(int sleep) {
            this.sleep = sleep;
        }

        public void run() {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                fail();
            }
        }
    }
}

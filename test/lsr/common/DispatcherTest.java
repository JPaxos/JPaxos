package lsr.common;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import junit.framework.Assert;
import lsr.common.Dispatcher.Priority;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class DispatcherTest {
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
        Runnable slowTask = new Runnable() {
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Assert.fail();
                }
            }
        };

        Runnable lowPriorityTask = mock(Runnable.class);
        Runnable highPriorityTask = mock(Runnable.class);

        dispatcher.dispatch(slowTask);
        dispatcher.dispatch(lowPriorityTask, Priority.Low);
        dispatcher.dispatch(highPriorityTask, Priority.High);

        Thread.sleep(200);

        InOrder inOrder = inOrder(lowPriorityTask, highPriorityTask);
        inOrder.verify(highPriorityTask).run();
        inOrder.verify(lowPriorityTask).run();
    }
}

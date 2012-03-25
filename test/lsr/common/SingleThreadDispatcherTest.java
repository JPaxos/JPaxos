package lsr.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class SingleThreadDispatcherTest {
    private SingleThreadDispatcher dispatcher;

    @Before
    public void setUp() {
        dispatcher = new SingleThreadDispatcher("test");
    }

    @Test
    public void shouldDeterminIfInDispatcherThread() {
        assertFalse(dispatcher.amIInDispatcher());

        dispatcher.executeAndWait(new Runnable() {
            public void run() {
                assertEquals(true, dispatcher.amIInDispatcher());
                dispatcher.checkInDispatcher();
            }
        });
    }

    @Test
    public void shouldExecuteTask() throws InterruptedException {
        Runnable task = mock(Runnable.class);

        dispatcher.executeAndWait(task);
        verify(task).run();
    }

    @Test
    public void shouldExecuteAndWaitIfInDispatcherThread() {
        final Runnable task1 = mock(Runnable.class);
        final Runnable task2 = mock(Runnable.class);
        final Runnable task3 = mock(Runnable.class);

        InOrder inOrder = inOrder(task1, task2, task3);
        dispatcher.executeAndWait(new Runnable() {
            public void run() {
                task1.run();
                dispatcher.executeAndWait(task2);
                task3.run();
            }
        });

        inOrder.verify(task1).run();
        inOrder.verify(task2).run();
        inOrder.verify(task3).run();
    }

    @Test
    public void shouldExecuteIfInDispatcherThread() {
        final Runnable task1 = mock(Runnable.class);
        final Runnable task2 = mock(Runnable.class);
        final Runnable task3 = mock(Runnable.class);

        InOrder inOrder = inOrder(task1, task2, task3);
        dispatcher.executeAndWait(new Runnable() {
            public void run() {
                task1.run();
                dispatcher.execute(task2);
                task3.run();
            }
        });

        inOrder.verify(task1).run();
        inOrder.verify(task3).run();
        inOrder.verify(task2).run();
    }
}

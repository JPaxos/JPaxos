package lsr.paxos.recovery;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import lsr.paxos.CatchUp;
import lsr.paxos.CatchUpListener;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RecoveryCatchUpTest {
    private Storage storage;
    private Log log;
    private CatchUp catchUp;
    private Runnable callback;
    private ArgumentCaptor<CatchUpListener> catchUpListener;

    @Before
    public void setUp() {
        log = mock(Log.class);
        storage = mock(Storage.class);
        when(storage.getLog()).thenReturn(log);
        catchUp = mock(CatchUp.class);
        callback = mock(Runnable.class);
        catchUpListener = ArgumentCaptor.forClass(CatchUpListener.class);
    }

    @Test
    public void shouldRecoverWhenNextIdIsZero() {
        when(storage.getFirstUncommitted()).thenReturn(0);
        RecoveryCatchUp recoveryCatchUp = new RecoveryCatchUp(catchUp, storage);
        recoveryCatchUp.recover(0, callback);

        verify(callback).run();
        verify(catchUp, never()).start();
    }

    @Test
    public void shouldRunCallbackAfterRecoveringAllInstances() {
        RecoveryCatchUp recoveryCatchUp = new RecoveryCatchUp(catchUp, storage);
        recoveryCatchUp.recover(5, callback);
        verify(callback, never()).run();
        verify(catchUp).addListener(catchUpListener.capture());
        verify(catchUp).start();
        verify(catchUp).startCatchup();
        verify(log).getInstance(4);

        // catch up retrieved 5 instances
        when(storage.getFirstUncommitted()).thenReturn(5);
        catchUpListener.getValue().catchUpSucceeded();

        verify(callback).run();
        verify(catchUp).removeListener(any(CatchUpListener.class));
    }

    @Test
    public void shouldWaitUntilAllInstancesAreRecovered() {
        RecoveryCatchUp recoveryCatchUp = new RecoveryCatchUp(catchUp, storage);
        recoveryCatchUp.recover(10, callback);
        verify(catchUp).addListener(catchUpListener.capture());
        verify(catchUp).start();
        verify(catchUp).startCatchup();
        verify(log).getInstance(9);

        // catch up retrieved only 9 instances
        when(storage.getFirstUncommitted()).thenReturn(9);
        catchUpListener.getValue().catchUpSucceeded();

        verify(callback, never()).run();
        verify(catchUp, never()).removeListener(any(CatchUpListener.class));
        verify(catchUp).forceCatchup();

        // catch up retrieved all required instances
        when(storage.getFirstUncommitted()).thenReturn(10);
        catchUpListener.getValue().catchUpSucceeded();

        verify(callback).run();
        verify(catchUp).removeListener(any(CatchUpListener.class));
    }
}

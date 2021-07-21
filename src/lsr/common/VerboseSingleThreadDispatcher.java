package lsr.common;

import java.lang.reflect.Field;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerboseSingleThreadDispatcher extends SingleThreadDispatcher {

    public VerboseSingleThreadDispatcher(String threadName) {
        super(threadName);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);

        try {
            Field f1 = FutureTask.class.getDeclaredField("callable");
            f1.setAccessible(true);
            Object callable = f1.get(r);

            Field f2 = callable.getClass().getDeclaredField("task");
            f2.setAccessible(true);
            Runnable task = (Runnable) f2.get(callable);

            logger.debug("Starting {}", task.getClass());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        logger.debug("Done");
        super.afterExecute(r, t);
    }

    static final Logger logger = LoggerFactory.getLogger(VerboseSingleThreadDispatcher.class);
}

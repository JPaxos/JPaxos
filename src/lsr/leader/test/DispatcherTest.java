package lsr.leader.test;

import java.util.concurrent.ExecutionException;

import lsr.common.Handler;
import lsr.common.SingleThreadDispatcher;

public class DispatcherTest {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        SingleThreadDispatcher dispacter = new SingleThreadDispatcher("TestThread");

        while (true) {
            dispacter.execute(new Handler() {
                @Override
                public void handle() {
                    System.out.println("Running");
                    throw new RuntimeException("Can't do that");

                }
            });
            Thread.sleep(1000);
        }
    }
}

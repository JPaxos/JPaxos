package lsr.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 
 * @author Nuno Santos (LSR)
 * @deprecated Use package lsr.paxos.statistics.
 */
public class PerformanceLogger {

    private final Thread writer;
    private final ArrayBlockingQueue<Log> queue = new ArrayBlockingQueue<Log>(512);
    private final BufferedWriter logFile;
    final Object lock = new Object();

    @SuppressWarnings("unused")
    private final String name;

    private final static Map<String, PerformanceLogger> loggers = new HashMap<String, PerformanceLogger>();

    public PerformanceLogger(String name) throws IOException {
        this.name = name;
        writer = new Thread(new Writer(), "Writer");
        writer.start();

        int i = 0;
        while (true) {
            File f = new File("perf-" + name + "-" + i + ".log");
            if (f.createNewFile()) {
                logFile = new BufferedWriter(new FileWriter(f), 8 * 1024);
                break;
            }
            i++;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    synchronized (lock) {
                        logFile.flush();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    final class Log {
        public final long ts;
        public final String msg;

        public Log(String msg) {
            this.ts = System.nanoTime();
            this.msg = msg;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder(msg.length() + 20);
            sb.append(ts / 1000).append(" ").append(msg);
            return sb.toString();
        }
    }

    final class Writer implements Runnable {
        public void run() {
            try {
                while (true) {
                    Log log = queue.take();
                    synchronized (lock) {
                        logFile.write(log.toString());
                        logFile.newLine();
                        logFile.flush();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    public void log(String msg) {
        // Fail if the queue is full
        queue.add(new Log(msg));
    }

    public static PerformanceLogger getLogger() {
        return getLogger("default");
    }

    public synchronized static PerformanceLogger getLogger(String name) {
        PerformanceLogger logger = loggers.get(name);
        if (logger == null) {
            try {
                logger = new PerformanceLogger(name);
                loggers.put(name, logger);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        return logger;
    }

    // static private PerformanceLogger logger = null;
}

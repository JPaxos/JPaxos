package lsr.paxos.statistics;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class PerformanceLogger {
    private static final int BUFFER_SIZE = 512 * 1024;

    private static final Map<String, PerformanceLogger> loggers = new HashMap<String, PerformanceLogger>();

    public static PerformanceLogger getLogger(String name) {
        try {
            PerformanceLogger logger;
            synchronized (loggers) {
                logger = loggers.get(name);
                if (logger == null) {
                    logger = new PerformanceLogger(name);
                }
                loggers.put(name, logger);
            }
            return logger;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    // Have a single shutdown hook for all loggers. Decreases the number of
    // threads created,
    // Important for clients nodes, where a single VM can have hundreds of
    // clients.
    private final static Thread shutdownThread;
    static {
        shutdownThread = new Thread() {
            public void run() {
                synchronized (loggers) {
                    for (PerformanceLogger pLogger : loggers.values()) {
                        pLogger.flush();
                    }
                }
            }
        };
        // Upon forced shutdown, ensure that buffered data is written to
        // the disk
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
    
    /* Per instance state */
    private final OutputStreamWriter fos;

    // TODO: Writing a message to the logger should have a minimal overhead.
    // Investigate what is the fastest way to write to a file in Java.
    private PerformanceLogger(String name) throws IOException {
        fos = new OutputStreamWriter(
                new BufferedOutputStream(
                        new FileOutputStream(name + ".stats.log", false), BUFFER_SIZE),
                Charset.forName("ISO-8859-1"));
    }

    public void log(String message) {
        try {
            fos.write(message);
        } catch (IOException e) {
            logger.warning("Cannot write performance data. " + e.getMessage());
        }
    }

    public void logln(String message) {
        try {
            fos.write(message);
            fos.write('\n');
        } catch (IOException e) {
            logger.warning("Cannot write performance data. " + e.getMessage());
        }
    }
    /**
     * 
     * @param duration in nanoseconds. Converted to microseconds before being
     *            written to the log (duration/1000)
     * @param message
     */
    public void log(long duration, String message) {
        StringBuilder sb = new StringBuilder(message.length() + 20);
        sb.append(duration / 1000).append('\t').append(message).append("\n");
        log(sb.toString());
    }

    public void flush() {
        try {
            fos.flush();
        } catch (IOException e) {
            logger.warning("Flush failed: " + e.getMessage());
        }
    }

    private final static Logger logger = Logger.getLogger(PerformanceLogger.class.getName());
}

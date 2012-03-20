package lsr.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Human readable formatter: date as HH:MM:SS instead of timestamp.
 * 
 * Format:
 * 
 * <code>
 * HH:MM:SS.mls <Thread> <class>.<method>() <Log message>
 * </code>
 * 
 * Example:
 * 
 * <code>
 * 12:16:45.154 Replica BenchmarkService.execute() Executing req: 33405
 * </code>
 * 
 * @author Nuno Santos (LSR)
 */
public class FullLogFormatter extends Formatter {

    /*
     * Keeps a per-thread pool of the auxiliary objects used to format the log
     * message.
     * 
     * The objects SimpleDateFormat and FieldPosition can be reused, which
     * should improve the logging performance. But they are not thread safe and
     * the log formatters may be accessed from multiple threads concurrently.
     */
    final static class State {
        public final SimpleDateFormat sdf = new SimpleDateFormat("kk:mm:ss.SSS");
        public final FieldPosition fpos = new FieldPosition(0);
    }

    private static final ThreadLocal<State> tl =
            new ThreadLocal<State>() {
                @Override
                protected State initialValue() {
                    return new State();
                }
            };

    /**
     * Format the given log record and return the formatted string.
     * 
     * @param record the log record to be formatted.
     * @return the formatted log record
     */
    public String format(LogRecord record) {
        State state = tl.get();
        SimpleDateFormat sdf = state.sdf;
        FieldPosition fpos = state.fpos;

        StringBuffer sb = new StringBuffer(128);
        // Appending to the string buffer should be significantly faster than
        // using String.format(). This method is on the critical path, so
        // must be made as fast as possible
        sdf.format(new Date(record.getMillis()), sb, fpos);
        sb.append(" ").append(Thread.currentThread().getName()).append(' ');
        // getSourceClassName() returns the fully classified name.
        // To reduce clutter, do not print the package name.
        String className = record.getSourceClassName();
        int index = className.lastIndexOf('.');
        if (index > 0) {
            className = className.substring(index + 1);
        }
        sb.append(className).append(".");
        sb.append(record.getSourceMethodName()).append("() ");
        sb.append(record.getMessage()).append('\n');
        printThrown(sb, record);
        return sb.toString();
    }

    public void printThrown(StringBuffer sb, LogRecord record) {
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            record.getThrown().printStackTrace(pw);
            pw.close();
            sb.append(sw.toString());
        }
    }
}

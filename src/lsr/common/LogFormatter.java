package lsr.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Format of log messages:
 * 
 * <code>
 * [<class>.<method>() <ts> <threadid>] msg
 * </code>
 * 
 * Example
 * 
 * <code>
 * [BenchmarkService.execute() 77460 Replica] Executing req: 2798
 * </code>
 */
public class LogFormatter extends Formatter {

    /**
     * Format the given log record and return the formatted string.
     * 
     * @param record the log record to be formatted.
     * @return the formatted log record
     */
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder();
        String className = record.getSourceClassName();
        int i = className.lastIndexOf('.');
        className = className.substring(i + 1);
        sb.append("[").append(className).append(".");
        sb.append(record.getSourceMethodName()).append("() ");
        sb.append(record.getMillis() % 100000).append(" ");
        sb.append(Thread.currentThread().getName()).append("] ");
        sb.append(record.getMessage());
        sb.append("\n");
        printThrown(sb, record);
        return sb.toString();
    }

    public void printThrown(StringBuilder sb, LogRecord record) {
        if (record.getThrown() != null) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                record.getThrown().printStackTrace(pw);
                pw.close();
                sb.append(sw.toString());
            } catch (Exception ex) {
                // Can't log the error to file, so write to console
                System.err.println("Error writing to log: " + ex);
            }
        }
    }
}

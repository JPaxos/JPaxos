package lsr.common.logFormatters;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class Benchmark2LogFormatter extends Formatter {

    /**
     * Format the given log record and return the formatted string.
     * 
     * @param record the log record to be formatted.
     * @return the formatted log record
     */
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append(record.getMillis());
        sb.append('\t');
        sb.append(record.getMessage());
        sb.append('\n');
        if (record.getThrown() != null) {
            printThrown(sb, record);
        }
        return sb.toString();
    }

    private void printThrown(StringBuilder sb, LogRecord record) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        pw.close();
        sb.append(sw.toString());
    }
}

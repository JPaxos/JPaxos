package lsr.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/** Intended for debugging, prints additional information and formats it in a more 
 * human-readable way (date as HH:MM:SS instead of timestamp).
 * 
 * @author Nuno Santos (LSR)
 *
 */
public class FullLogFormatter extends Formatter {
 
    private final SimpleDateFormat sdf = 
//    new SimpleDateFormat("kk:mm:ss.SSS");
		new SimpleDateFormat("kk:mm:ss");
    private final FieldPosition fpos = new FieldPosition(0);

    /**
     * Format the given log record and return the formatted string.
     * 
     * @param record the log record to be formatted.
     * @return the formatted log record
     */
    public String format(LogRecord record) {
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

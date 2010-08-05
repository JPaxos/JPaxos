package lsr.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class MyLogFormatter extends Formatter {

	// private final SimpleDateFormat sdf = new
	// SimpleDateFormat("kk:mm:ss.SSS");
	// private final FieldPosition fpos = new FieldPosition(0);

	/**
	 * Format the given log record and return the formatted string.
	 * 
	 * @param record
	 *            the log record to be formatted.
	 * @return the formatted log record
	 */
	public String format(LogRecord record) {
		StringBuffer sb = new StringBuffer(128);
		// Appending to the string buffer should be significantly faster than
		// using String.format(). This method is on the critical path, so
		// must be made as fast as possible
		sb.append(record.getMillis() % 100000000).append(' ');
		// sdf.format(new Date(record.getMillis()), sb, fpos);
		// sb.append(String.format(" %-12s %-15s ",
		// Thread.currentThread().getName(),
		// getMethod(record)));
		sb.append(Thread.currentThread().getName()).append(' ');
		// sb.append(record.getSourceClassName()).append(".");
		// sb.append(record.getSourceMethodName()).append("() ");
		sb.append("nil ");
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

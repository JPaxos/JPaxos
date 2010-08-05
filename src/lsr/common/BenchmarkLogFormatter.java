package lsr.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class BenchmarkLogFormatter extends Formatter {
	
	private static final long INIT_TIME = System.currentTimeMillis();
	
	/**
	 * Format the given log record and return the formatted string.
	 * 
	 * @param record
	 *            the log record to be formatted.
	 * @return the formatted log record
	 */
	public String format(LogRecord record) {
		StringBuilder sb = new StringBuilder();
//		sb.append(String.format("%-25s %s", record.getMillis(), record.getMessage()));
		sb.append(String.format("%-15s %s\n", record.getMillis() - INIT_TIME, record.getMessage()));		
		printThrown(sb, record);
		return sb.toString();
	}

	private void printThrown(StringBuilder sb, LogRecord record) {
		if (record.getThrown() != null) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			record.getThrown().printStackTrace(pw);
			pw.close();
			sb.append(sw.toString());
		}
	}
}

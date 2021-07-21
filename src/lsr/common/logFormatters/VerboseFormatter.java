package lsr.common.logFormatters;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.LayoutBase;

public class VerboseFormatter extends LayoutBase<ILoggingEvent> {

    protected final DateFormat dateFormat = new SimpleDateFormat("mm:ss.SSS");

    private void printThrown(StringBuilder sb, ILoggingEvent event) {
        IThrowableProxy itp = event.getThrowableProxy();
        if (itp instanceof ThrowableProxy) {
            ThrowableProxy tp = (ThrowableProxy) itp;
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            tp.getThrowable().printStackTrace(pw);
            pw.close();
            sb.append(sw.toString());
        } else {
            sb.append("!!! :: SOME UNPARSABLE ERROR :: !!!");
            sb.append(itp.getMessage());
        }
    }

    @Override
    public String doLayout(ILoggingEvent event) {
        String loggerName = event.getLoggerName();
        int lastDot = loggerName.lastIndexOf('.');
        if (lastDot != -1)
            loggerName = loggerName.substring(lastDot + 1);
        loggerName = String.format("%-21s", loggerName);

        Date ts = new Date(event.getTimeStamp());

        StringBuilder sb = new StringBuilder();

        sb.append(dateFormat.format(ts));
        sb.append("  ");
        if (event.getLevel().levelInt >= Level.WARN_INT)
            sb.append("\033[31m");
        if (event.getLevel().levelInt <= Level.DEBUG_INT)
            sb.append("\033[37m");
        sb.append(loggerName);
        sb.append("  ");
        sb.append(event.getFormattedMessage());
        sb.append('\n');
        if (event.getLevel().levelInt >= Level.WARN_INT ||
            event.getLevel().levelInt <= Level.DEBUG_INT)
            sb.append("\033[00m");

        if (event.getThrowableProxy() != null) {
            printThrown(sb, event);
        }

        return sb.toString();
    }
}

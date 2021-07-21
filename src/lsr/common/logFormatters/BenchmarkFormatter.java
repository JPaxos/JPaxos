package lsr.common.logFormatters;

import java.io.PrintWriter;
import java.io.StringWriter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.LayoutBase;

public class BenchmarkFormatter extends LayoutBase<ILoggingEvent> {

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
        StringBuilder sb = new StringBuilder();
        // I guess the timestamp is in milliseconds. This is a guess, as the
        // documentations does not specify a unit. This is javaaa
        long ts = event.getTimeStamp();
        sb.append(String.format("%d.%03d", ts / 1000, ts % 1000));
        sb.append(' ');
        sb.append(event.getFormattedMessage());
        sb.append('\n');
        if (event.getThrowableProxy() != null) {
            printThrown(sb, event);
        }
        return sb.toString();
    }
}

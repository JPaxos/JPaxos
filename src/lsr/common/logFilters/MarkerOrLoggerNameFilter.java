package lsr.common.logFilters;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;

public class MarkerOrLoggerNameFilter extends TurboFilter {

    String marker;
    private Marker markerToAccept;

    String loggerName;

    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level, String format,
                              Object[] params, Throwable t) {
        if (!isStarted()) {
            return FilterReply.NEUTRAL;
        }

        if ((markerToAccept.equals(marker))) {
            return FilterReply.ACCEPT;
        }

        if (logger.getName().equals(getLoggerName())) {
            return FilterReply.ACCEPT;
        }

        return FilterReply.DENY;

    }

    @Override
    public void start() {
        if (marker == null) {
            addError("Missing marker name");
            marker = "";
        }

        if (loggerName == null) {
            addError("Missing logger name");
            marker = "loggerName";
        }

        markerToAccept = MarkerFactory.getMarker(marker);

        super.start();
    }

    public String getMarker() {
        return marker;
    }

    public void setMarker(String markerStr) {
        this.marker = markerStr;
    }

    public String getLoggerName() {
        return loggerName;
    }

    public void setLoggerName(String loggerName) {
        this.loggerName = loggerName;
    }
}

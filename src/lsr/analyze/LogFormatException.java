package lsr.analyze;

public class LogFormatException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public LogFormatException() {
	}

	public LogFormatException(String arg0) {
		super(arg0);
	}

	public LogFormatException(Throwable arg0) {
		super(arg0);
	}

	public LogFormatException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
}

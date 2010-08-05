package put.consensus;

/**
 * Here be wrapped all exceptions concerning storage.
 * 
 * @author Jan K
 */
public class StorageException extends Exception {

	public StorageException(Throwable cause) {
		super(cause);
	}

	public StorageException(String message, Throwable cause) {
		super(message, cause);
	}

	private static final long serialVersionUID = 1L;
}

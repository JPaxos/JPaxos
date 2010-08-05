package lsr.common;

/**
 * Represents request which should not be executed on machine state. This
 * request is created only when leader doesn't know value of some instance but
 * he have to propose something. Machine state should ignore this request.
 * 
 * @see Request
 */
public class NoOperationRequest extends Request {
	private static final long serialVersionUID = 1L;

	/**
	 * Creates new empty no operation request.
	 */
	public NoOperationRequest() {
		super(null, null);
	}

	public byte[] toByteArray() {
		return new byte[] { 0, 0, 0, 0 };
	}
}

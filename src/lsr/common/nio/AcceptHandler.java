package lsr.common.nio;

/**
 * Represents classes which are responsible for accepting connections. Every
 * registered handler in selector, will be notified when there is waiting
 * connection on underlying channel. This is used mainly by
 * <code>SelectorThread</code>.
 * 
 * @see SelectorThread
 */
public interface AcceptHandler {
    /**
     * Called by <code>SelectorThread</code> every time new connection can be
     * accepted.
     */
    void handleAccept();
}

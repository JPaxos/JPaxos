package process_handlers;

public interface ClientHandler extends ProcessHandler {
	void sendRequests(int count, long delay, boolean randomDelay);

	String getName();

	void setName(String name);
}

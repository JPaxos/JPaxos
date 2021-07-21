package process_handlers;

import benchmark.ProcessListener;
import commands.Command;

public interface ProcessHandler {
	void setLastCommand(Command c);

	Command getLastCommand();

	void stop();

	void kill();

	void addProcessListener(ProcessListener procesListener);

	String getLaunchCommand();
}

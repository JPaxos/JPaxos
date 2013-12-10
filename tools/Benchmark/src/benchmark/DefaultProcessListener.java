package benchmark;

import commands.ClientCommand;
import commands.Command;
import commands.ReplicaCommand;

import process_handlers.ProcessHandler;

public class DefaultProcessListener implements ProcessListener {

	Benchmark benchmark;
	Scheduler scheduler;
	private final MyLogger logger;

	public DefaultProcessListener(Benchmark benchmark, Scheduler scheduler, MyLogger logger) {
		this.benchmark = benchmark;
		this.scheduler = scheduler;
		this.logger = logger;
	}

	@Override
	public void processFinished(ProcessHandler processHandler) {		
		logger.processStopped(processHandler);
		benchmark.processStopped(processHandler);
		checkEvent(processHandler);
	}

	@Override
	public void errorCaught(ProcessHandler processHandler) {
		logger.errorCaught(processHandler);
		benchmark.forceStop();
	}

	@Override
	public void clientSent(ProcessHandler processHandler) {
		logger.clientSent(processHandler);
		checkEvent(processHandler);
	}

	private synchronized void checkEvent(ProcessHandler processHandler) {
		Command c = processHandler.getLastCommand();
		if (c == null)
			return;
		String event = c.eventName();
		if (event == null)
			return;

		if (c instanceof ClientCommand) {
			((ClientCommand) c).numberOfTargets--;
			if (((ClientCommand) c).numberOfTargets == 0) {
				scheduler.eventOccured(event);
			}
		} else if (c instanceof ReplicaCommand)
			scheduler.eventOccured(event);
	}

	public void processCreated(ProcessHandler processHandler) {
		checkEvent(processHandler);
	}

}

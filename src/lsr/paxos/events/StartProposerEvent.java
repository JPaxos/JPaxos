package lsr.paxos.events;

import lsr.paxos.Proposer;

public class StartProposerEvent implements Runnable {
	private Proposer proposer;

	public StartProposerEvent(Proposer proposer) {
		this.proposer = proposer;
	}

	public void run() {
		// logger.fine("Proposer starting.");
		proposer.prepareNextView();
	}

	// private final static Logger logger =
	// Logger.getLogger(StartProposerEvent.class.getCanonicalName());
}

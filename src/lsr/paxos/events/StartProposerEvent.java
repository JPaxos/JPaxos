package lsr.paxos.events;

import lsr.paxos.Proposer;

public class StartProposerEvent implements Runnable {
	private Proposer _proposer;

	public StartProposerEvent(Proposer proposer) {
		_proposer = proposer;
	}

	public void run() {
//		logger.fine("Proposer starting.");
		_proposer.prepareNextView();
	}

//	private final static Logger logger = Logger.getLogger(StartProposerEvent.class.getCanonicalName());
}

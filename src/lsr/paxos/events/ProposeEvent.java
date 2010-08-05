package lsr.paxos.events;

import java.util.logging.Logger;

import lsr.common.Request;
import lsr.paxos.Proposer;
import lsr.paxos.Proposer.ProposerState;

public class ProposeEvent implements Runnable {
	private final Request _value;
	private final Proposer _proposer;

	public ProposeEvent(Proposer proposer, Request value) {
		_proposer = proposer;
		_value = value;
	}

	public void run() {
		if (_proposer.getState() == ProposerState.INACTIVE)
			logger.warning("Executing propose event on INACTIVE proposer.");
		else {
//			logger.warning("Proposing: " + _value);
			_proposer.propose(_value);
		}
	}

	private final static Logger logger = Logger.getLogger(ProposeEvent.class.getCanonicalName());
}
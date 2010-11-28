package lsr.paxos.events;

import java.util.logging.Logger;

import lsr.common.Request;
import lsr.paxos.Proposer;
import lsr.paxos.Proposer.ProposerState;

public class ProposeEvent implements Runnable {
    private final Request value;
    private final Proposer proposer;

    public ProposeEvent(Proposer proposer, Request value) {
        this.proposer = proposer;
        this.value = value;
    }

    public void run() {
        if (proposer.getState() == ProposerState.INACTIVE)
            logger.warning("Executing propose event on INACTIVE proposer.");
        else {
            // logger.warning("Proposing: " + _value);
            proposer.propose(value);
        }
    }

    private final static Logger logger = Logger.getLogger(ProposeEvent.class.getCanonicalName());
}
package lsr.paxos.test;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.PaxosImpl;

public class LeaderPromoter {
    private final PaxosImpl paxos;
    private final SingleThreadDispatcher dispatcher;
    private final int localId = ProcessDescriptor.getInstance().localId;
    private final int n = ProcessDescriptor.getInstance().numReplicas;
    
    private int counter=0;

    public LeaderPromoter(PaxosImpl paxos) {
        this.paxos = paxos;
        dispatcher = paxos.getDispatcher();
        dispatcher.scheduleAtFixedRate(new PromoteTask(), 10, 5, TimeUnit.SECONDS);
        dispatcher.start();
        logger.info("Starting");
    }

    class PromoteTask implements Runnable {
        // Execute on the Protocol thread.
        @Override
        public void run() {
            counter++;
            logger.info("Counter: " + counter);
            int view = paxos.getStorage().getView();
            if (counter % n == localId) {
                if (paxos.isLeader()) {
                    logger.warning("View: " + view + " - Already leader. Will not promote.");
                } else {
                    logger.warning("View: " + view + " - Promoting local process to leader.");
                    paxos.startProposer();
                }
            }
        }
    }

    private final static Logger logger = Logger.getLogger(LeaderPromoter.class.getCanonicalName());
}

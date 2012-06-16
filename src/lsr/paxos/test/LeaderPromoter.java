package lsr.paxos.test;

import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.ProcessDescriptor;
import lsr.paxos.core.PaxosImpl;
import lsr.paxos.network.TcpNetwork;

final public class LeaderPromoter {
    private final PaxosImpl paxos;
    private final Dispatcher dispatcher;
    private final int localId;
    private final int n;

    public static final String TEST_LEADERPROMOTER_INTERVAL = "test.LeaderPromoter.Interval";
    public static final int DEFAULT_TEST_LEADERPROMOTER_INTERVAL = 5000;
    private final int leaderPromoterInterval;

    private int counter = 0;

    public LeaderPromoter(PaxosImpl paxos) {
        this.paxos = paxos;
        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        this.localId = pd.localId;
        this.n = pd.numReplicas;

        this.leaderPromoterInterval = pd.config.getIntProperty(
                TEST_LEADERPROMOTER_INTERVAL, DEFAULT_TEST_LEADERPROMOTER_INTERVAL);
        logger.warning(TEST_LEADERPROMOTER_INTERVAL + " = " + leaderPromoterInterval);

        dispatcher = paxos.getDispatcher();
        // Wait 10s before the first promotion
        // dispatcher.scheduleAtFixedRate(new PromoteTask(), 10000,
        // leaderPromoterInterval, TimeUnit.MILLISECONDS);
        dispatcher.schedule(new CrashTask(), 20000);
    }

    final class PromoteTask implements Runnable {
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

    final class CrashTask implements Runnable {
        @Override
        public void run() {
            if (paxos.isLeader()) {
                // Kills the replica with id (leader+1) % n
                // if (((paxos.getLeaderId() + 1) %
                // ProcessDescriptor.getInstance().numReplicas) == localId) {
                logger.warning("Going harakiri");
                TcpNetwork net = (TcpNetwork) paxos.getNetwork();
                net.closeAll();
                System.exit(1);
            }
        }

    }

    private final static Logger logger = Logger.getLogger(LeaderPromoter.class.getCanonicalName());
}

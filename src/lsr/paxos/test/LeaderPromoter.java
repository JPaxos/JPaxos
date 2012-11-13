package lsr.paxos.test;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.network.TcpNetwork;
import lsr.paxos.replica.ClientBatchManager;

final public class LeaderPromoter {
    private final Paxos paxos;
    private final SingleThreadDispatcher dispatcher;
    private final int localId;
    private final int n;

    public static final String TEST_LEADERPROMOTER_INTERVAL = "test.LeaderPromoter.Interval";
    public static final int DEFAULT_TEST_LEADERPROMOTER_INTERVAL = 5000;
    private final int leaderPromoterInterval;

    private int counter = 0;

    public LeaderPromoter(Paxos paxos) {
        this.paxos = paxos;
        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        this.localId = pd.localId;
        this.n = pd.numReplicas;

        this.leaderPromoterInterval = pd.config.getIntProperty(
                TEST_LEADERPROMOTER_INTERVAL, DEFAULT_TEST_LEADERPROMOTER_INTERVAL);
        logger.warning(TEST_LEADERPROMOTER_INTERVAL + " = " + leaderPromoterInterval);

        dispatcher = paxos.getDispatcher();
        // Wait 10s before the first promotion
        dispatcher.scheduleAtFixedRate(new PromoteTask(), 10000, leaderPromoterInterval,
                TimeUnit.MILLISECONDS);
        // dispatcher.schedule(new CrashTask(), 20000, TimeUnit.MILLISECONDS);
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
            logger.warning("Crash task executing. " +
                           paxos.getLeaderId() +
                           ", "
                           +
                           ((paxos.getLeaderId() + 1) % ProcessDescriptor.getInstance().numReplicas));

            if (paxos.isLeader()) {
                // Kills the replica with id (leader+1) % n
                // if (((paxos.getLeaderId() + 1) %
                // ProcessDescriptor.getInstance().numReplicas) == localId) {
                logger.warning("Going harakiri");
                TcpNetwork net = (TcpNetwork) paxos.getNetwork();
                ProposerImpl proposer = (ProposerImpl) paxos.getProposer();
                ClientBatchManager cliBatch = proposer.getClientBatchManager();
                cliBatch.cleanStop();
                // Wait for all messages to be sent
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                net.closeAll();
                System.exit(1);
            }
        }

    }

    private final static Logger logger = Logger.getLogger(LeaderPromoter.class.getCanonicalName());
}

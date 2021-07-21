package lsr.paxos.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.paxos.CatchUpListener;
import lsr.paxos.core.CatchUp;
import lsr.paxos.storage.Storage;

import static lsr.common.ProcessDescriptor.processDescriptor;

/**
 * Represents <code>CatchUp</code> wrapper used in recovery algorithms to
 * retrieve all decided instances up to specified value. The
 * <code>CatchUp</code> mechanism is used to retrieve instances from other
 * replicas and save them to <code>Storage</code>.
 */
public class RecoveryCatchUp {
    private final Storage storage;
    private final CatchUp catchUp;

    /**
     * Creates new instance of <code>RecoveryCatchUp</code> class.
     * 
     * @param catchUp - the catch-up mechanism used to retrieve required
     *            instances
     * @param storage - the storage with paxos state
     */
    public RecoveryCatchUp(CatchUp catchUp, Storage storage) {
        this.storage = storage;
        this.catchUp = catchUp;
    }

    /**
     * Retrieves all instances before <code>firstUncommitted</code> from other
     * replicas and save them to storage. The <code>callback</code> is executed
     * when all required instances are retrieved (when first uncommitted
     * instance in storage is greater or equal than
     * <code>firstUncommitted</code>).
     * 
     * @param firstUncommitted - the minimum required value of first uncommitted
     * @param callback - the callback executed when recovering is finished
     */
    public void recover(final int firstUncommitted, final Runnable callback) {
        if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
            logger.warn(processDescriptor.logMark_Benchmark2019, "CRB");
        if (storage.getFirstUncommitted() >= firstUncommitted) {
            logger.info("Recovery catch-up unnecessary, running callback");
            if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
                logger.warn(processDescriptor.logMark_Benchmark2019, "CRE");
            callback.run();
            return;
        }

        storage.getLog().getInstance(firstUncommitted - 1);

        catchUp.addListener(new CatchUpListener() {
            public void catchUpAdvanced() {
                if (storage.getFirstUncommitted() >= firstUncommitted) {
                    logger.info("Recovery catch-up succeeded");
                    catchUp.removeListener(this);
                    if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
                        logger.warn(processDescriptor.logMark_Benchmark2019, "CRE");
                    callback.run();
                } else {
                    catchUp.forceCatchup();
                }
            }
        });

        logger.info("Starting recovery catch-up up to {}", firstUncommitted);

        catchUp.forceCatchup();
    }

    private final static Logger logger = LoggerFactory.getLogger(RecoveryCatchUp.class);
}

package lsr.paxos.idgen;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Constructs ID as composite of number and view/epoch
 * 
 * a long number 0x0123456789abcdef is divided (at lenght of {@link #SHIFT}) to
 * two parts - one for view/epoch, other for sequence no (as in
 * {@link SimpleIdGenerator}).
 * 
 * Eg:
 * 
 * | 01 23 45 | 67 89 ab cd ef |
 * 
 * |view/epoch| . . clientNo . |
 * 
 * This class guarantees that client ID's remain correct unless either
 * 
 * 2^(62-SHIFT) - currently 16 million - view / epoch changes happen
 * 
 * or
 * 
 * 2^SHIFT/N - currently 10 milliard for 11 replicas - clients call for ID in
 * the same view/epoch
 */
public class ViewEpochIdGenerator implements IdGenerator {

    /** initial view or epoch of the replica (at the moment of replica start) */
    private final long viewOrEpochShifted;

    /** SHIFT bytes for unique client ID, 64-SHIFT bytes for view/epoch */
    private static final int SHIFT = 40;

    private final AtomicLong current = new AtomicLong(processDescriptor.localId);

    public ViewEpochIdGenerator(long viewOrEpoch) {
        this.viewOrEpochShifted = viewOrEpoch << SHIFT;
    }

    @Override
    public long next() {
        long base = current.addAndGet(processDescriptor.numReplicas);

        return viewOrEpochShifted | base;
    }

}

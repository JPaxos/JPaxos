package lsr.paxos.replica;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>A client id generator that starts by generating small 
 * numbers. Having small client ids can be convenient for 
 * debugging and for some specific situations. 
 *  
 * <p> If used correctly, this generator will ensure unique 
 * ids. If replicas are not allowed to recover, then it is 
 * enough to initialize the generator to:
 * 
 * <code>new SimpleIdGenerator(replicaID, nReplicas)</code>
 * 
 * <p> The generated ids will be of the form 
 * <code>replicaID + k*nReplicas</code>, which ensures that 
 * each replica generates its own unique stream of ids.
 * 
 * <p>If replicas are allowed to recover, then ids are not
 * guaranteed to be unique after recovery. To prevent this,
 * the replica should keep on stable storage the last id
 * that it generated and when recovering initialize
 * this class to start from this id. This is not implemented.
 * 
 */
public class SimpleIdGenerator implements IdGenerator {
	private final long step;
	private final AtomicLong current;

	/**
	 * Initialize new instance of <code>SimpleIdGenerator</code>.
	 * 
	 * @param start
	 *            - first number in generated sequence
	 * @param step
	 *            - difference between next's id's
	 * 
	 */
	public SimpleIdGenerator(long start, long step) {
		current = new AtomicLong(start);
		this.step = step;
	}

	/**
	 * Generates next number from the sequence specified by this generator.
	 * 
	 * This method is thread-safe.
	 * 
	 * @return next unique number
	 */
	public long next() {
		return current.getAndAdd(step);
	}
}

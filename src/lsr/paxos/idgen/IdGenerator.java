package lsr.paxos.idgen;

/**
 * The <code>IdGenerator</code> interface provide method for generating unique
 * id's.
 * 
 */
public interface IdGenerator {
    /**
     * Generates next free id. Each generated id from this method should be
     * unique (every call of this method returns different value). This method
     * should be thread-safe.
     * 
     * @return next free id
     */
    long next();
}
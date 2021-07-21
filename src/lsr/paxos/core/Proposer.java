package lsr.paxos.core;

import java.lang.annotation.Native;

import lsr.paxos.messages.PrepareOK;

public interface Proposer {

    @Native
    public static final byte ENUM_PROPOSERSTATE_INACTIVE = 1;
    @Native
    public static final byte ENUM_PROPOSERSTATE_PREPARING = 2;
    @Native
    public static final byte ENUM_PROPOSERSTATE_PREPARED = 3;

    public enum ProposerState {
        INACTIVE, PREPARING, PREPARED
    }

    public void start();

    public ProposerState getState();

    public void ballotFinished();

    public void stopProposer();

    public void onPrepareOK(PrepareOK msg, int sender);

    public void propose(byte[] value);

    public void prepareNextView();

    /**
     * After reception of majority accepts, we suppress propose messages.
     * 
     * @param instanceId no. of instance, for which we want to stop
     *            retransmission
     */
    public void stopPropose(int instanceId);

    /**
     * If retransmission to some process for certain instance is no longer
     * needed, we should stop it
     * 
     * @param instanceId no. of instance, for which we want to stop
     *            retransmission
     * @param destination number of the process in processes PID list
     */
    public void stopPropose(int instanceId, int destination);

    /**
     * When proposing is fast and executing slow, then proposer stops proposing;
     * this tells the proposes that it might go on.
     * 
     * Warning: this is called from the replica thread!
     */
    public void instanceExecuted(int instanceId);

    interface OnLeaderElectionResultTask {
        void onPrepared();

        void onFailedToPrepare();
    }

    /**
     * Schedules a task to be executed as soon as the proposer is prepared
     */
    public void executeOnPrepared(final OnLeaderElectionResultTask task);

}
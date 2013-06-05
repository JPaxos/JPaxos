package lsr.paxos.recovery;

import java.util.BitSet;
import java.util.logging.Logger;

import lsr.paxos.core.Paxos;
import lsr.paxos.core.Proposer;
import lsr.paxos.core.Proposer.ProposerState;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.storage.Storage;

public class ViewRecoveryRequestHandler implements MessageHandler {
    private final Paxos paxos;

    public ViewRecoveryRequestHandler(Paxos paxos) {
        this.paxos = paxos;
    }

    public void onMessageReceived(Message msg, final int sender) {
        final Recovery recovery = (Recovery) msg;

        paxos.getDispatcher().submit(new Runnable() {
            public void run() {
                logger.info("Received " + recovery);

                Storage storage = paxos.getStorage();
                Proposer proposer = paxos.getProposer();

                if (paxos.getLeaderId() == sender ||
                    recovery.getView() >= storage.getView()) {
                    /*
                     * if current leader is recovering, we cannot respond and we
                     * should change a leader
                     */

                    // OR

                    /*
                     * The recovering process notified me that it crashed in
                     * view recovery.getView()
                     * 
                     * This view is not less then current. View change must be
                     * performed.
                     */
                    if (proposer.getState() != ProposerState.INACTIVE)
                        proposer.stopProposer();
                    proposer.prepareNextView();

                    // reschedule receiving msg
                    onMessageReceived(recovery, sender);
                    return;
                }

                if (paxos.isLeader() &&
                    proposer.getState() == ProposerState.PREPARING) {
                    // wait until we prepare the view
                    proposer.executeOnPrepared(new Proposer.Task() {

                        public void onPrepared() {
                            onMessageReceived(recovery, sender);
                        }

                        public void onFailedToPrepare() {
                            onMessageReceived(recovery, sender);
                        }
                    });
                    return;
                }

                RecoveryAnswer answer = new RecoveryAnswer(storage.getView(),
                        storage.getLog().getNextId());
                paxos.getNetwork().sendMessage(answer, sender);
            }
        });
    }

    public void onMessageSent(Message message, BitSet destinations) {
    }

    private static final Logger logger = Logger.getLogger(ViewRecoveryRequestHandler.class.getCanonicalName());
}
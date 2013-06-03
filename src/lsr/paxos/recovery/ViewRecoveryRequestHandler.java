package lsr.paxos.recovery;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;
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

                if (paxos.getLeaderId() == sender) {
                    // if current leader is recovering, we cannot respond
                    // and we should change a leader

                    paxos.suspect(paxos.getLeaderId());
                    paxos.getDispatcher().schedule(new Runnable() {

                        @Override
                        public void run() {
                            onMessageReceived(recovery, sender);
                        }
                    }, 1, TimeUnit.MILLISECONDS);
                    return;
                }

                if (paxos.isLeader() &&
                    paxos.getProposer().getState() == ProposerState.PREPARING) {
                    // wait until we prepare the view
                    paxos.getProposer().executeOnPrepared(new Proposer.Task() {

                        public void onPrepared() {
                            onMessageReceived(recovery, sender);
                        }

                        public void onFailedToPrepare() {
                            onMessageReceived(recovery, sender);
                        }
                    });
                    return;
                }

                Storage storage = paxos.getStorage();

                if (recovery.getView() >= storage.getView()) {
                    /*
                     * The recovering process notified me that it crashed in
                     * view recovery.getView()
                     * 
                     * This view is not less then current. View change must be
                     * performed.
                     */
                    int newView = recovery.getView() + 1;
                    if (processDescriptor.isLocalProcessLeader(newView)) {
                        newView++;
                    }
                    paxos.advanceView(newView);
                    paxos.suspect(newView);

                    // reschedule receiving msg
                    onMessageReceived(recovery, sender);
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
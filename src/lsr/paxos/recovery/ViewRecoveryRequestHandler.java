package lsr.paxos.recovery;

import java.util.BitSet;

import lsr.paxos.Paxos;
import lsr.paxos.Proposer.ProposerState;
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
                if (paxos.getLeaderId() == sender) {
                    // if current leader is recovering, we cannot respond
                    // and we should change a leader
                    // TODO TZ - to increase recovery performance, force
                    // changing view instead of waiting for failure detector
                    return;
                }

                if (paxos.isLeader() &&
                    paxos.getProposer().getState() == ProposerState.PREPARING) {
                    // wait until we prepare the view
                    return;
                }

                Storage storage = paxos.getStorage();

                if (recovery.getView() > storage.getView()) {
                    paxos.advanceView(recovery.getView());
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
}
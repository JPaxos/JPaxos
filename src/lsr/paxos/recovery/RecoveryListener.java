package lsr.paxos.recovery;

import lsr.paxos.Paxos;
import lsr.paxos.storage.PublicDiscWriter;

public interface RecoveryListener {
    void recoveryFinished(Paxos paxos, PublicDiscWriter publicDiscWriter);
}

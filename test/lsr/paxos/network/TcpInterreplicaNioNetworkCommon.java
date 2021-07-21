package lsr.paxos.network;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;

import lsr.common.ClientRequest;
import lsr.common.Configuration;
import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.common.Range;
import lsr.common.Reply;
import lsr.paxos.Snapshot;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.CatchUpQuery;
import lsr.paxos.messages.CatchUpResponse;
import lsr.paxos.messages.CatchUpSnapshot;
import lsr.paxos.messages.ForwardClientRequests;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.storage.InMemoryConsensusInstance;

public class TcpInterreplicaNioNetworkCommon {

    protected PID pid0;
    protected PID pid1;

    @Before
    public void setUp() {
        pid0 = new PID(0, "localhost", 3000, 0);
        pid1 = new PID(1, "localhost", 3001, 0);
        List<PID> processes = new ArrayList<PID>();
        processes.add(pid0);
        processes.add(pid1);

        Configuration configuration = new Configuration(processes);
        ProcessDescriptor.initialize(configuration, 0);
    }

    public static ArrayList<Message> makeMessages() {
        ArrayList<Message> messages = new ArrayList<Message>();
        messages.add(new Recovery(1, 2));
        messages.add(new RecoveryAnswer(3, new long[] {4, 5}, 6));
        messages.add(new Prepare(7, 8));
        InMemoryConsensusInstance inst1 = new InMemoryConsensusInstance(9);
        InMemoryConsensusInstance inst2 = new InMemoryConsensusInstance(10);
        inst1.updateStateFromPropose(1, 11, new byte[] {'a', 'b'});
        inst2.updateStateFromDecision(12, new byte[] {'c', 'd'});
        messages.add(new PrepareOK(13, new InMemoryConsensusInstance[] {inst1, inst2},
                new long[] {14, 15}));
        messages.add(new Propose(16, 17, new byte[] {'e', 'f', 'g'}));
        messages.add(new Accept(18, 19));
        messages.add(new Alive(20, 21));
        messages.add(new CatchUpQuery(22, new int[] {23, 24}, new Range[] {new Range(25, 26)}));
        InMemoryConsensusInstance inst3 = new InMemoryConsensusInstance(10);
        inst3.updateStateFromDecision(27, new byte[] {'h'});
        messages.add(new CatchUpResponse(28, 29l,
                Arrays.asList(new InMemoryConsensusInstance[] {inst2, inst3})));
        Snapshot snapshot = new Snapshot();
        snapshot.setLastReplyForClient(new HashMap<Long, Reply>());
        snapshot.setNextInstanceId(30);
        snapshot.setNextRequestSeqNo(31);
        snapshot.setPartialResponseCache(new ArrayList<Reply>());
        snapshot.setStartingRequestSeqNo(32);
        snapshot.setValue(new byte[] {'i', 'j', 'l', 'm'});
        messages.add(new CatchUpSnapshot(33, 34l, snapshot));
        messages.add(new ForwardClientRequests(new ClientRequest[] {}));
        messages.add(new Recovery(35, 36));
        return messages;
    }

}
package lsr.paxos;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import lsr.common.ProcessDescriptor;
import lsr.common.ProcessDescriptorHelper;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.paxos.Proposer.ProposerState;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.recovery.MockDispatcher;
import lsr.paxos.replica.Replica.CrashModel;
import lsr.paxos.statistics.ReplicaStats;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ProposerImplTest {
    private MockDispatcher dispatcher;
    private Paxos paxos;
    private Network network;
    private PassiveFailureDetector failureDetector;
    private Storage storage;

    @Before
    public void setUp() throws IOException {
        ProcessDescriptorHelper.initialize(3, 0);
        ReplicaStats.initialize(3, 0);

        dispatcher = new MockDispatcher();

        paxos = mock(Paxos.class);
        network = mock(Network.class);
        failureDetector = mock(PassiveFailureDetector.class);
        storage = new InMemoryStorage();

        when(paxos.getDispatcher()).thenReturn(dispatcher);
        when(paxos.isLeader()).thenReturn(true);
    }

    @Test
    public void shouldBeInactiveAfterCreation() {
        Proposer proposer = new ProposerImpl(paxos, network, failureDetector, storage,
                CrashModel.CrashStop);

        assertEquals(ProposerState.INACTIVE, proposer.getState());
    }

    @Test
    public void shouldPrepareNextView() {
        storage.setView(5);

        final Proposer proposer = new ProposerImpl(paxos, network, failureDetector, storage,
                CrashModel.CrashStop);
        dispatcher.dispatch(new Runnable() {
            public void run() {
                proposer.prepareNextView();
            }
        });
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network).sendMessage(messageArgument.capture(), any(BitSet.class));
        Prepare prepare = (Prepare) messageArgument.getValue();

        assertEquals(6, prepare.getView());

        assertEquals(ProposerState.PREPARING, proposer.getState());
    }

    @Test
    public void shouldHandlePrepareOk() {
        final Proposer proposer = new ProposerImpl(paxos, network, failureDetector, storage,
                CrashModel.CrashStop);
        dispatcher.dispatch(new Runnable() {
            public void run() {
                proposer.prepareNextView();
            }
        });
        dispatcher.execute();

        ConsensusInstance instance0 = new ConsensusInstance(0, LogEntryState.DECIDED, 3,
                new byte[] {1});
        ConsensusInstance instance1 = new ConsensusInstance(1);
        ConsensusInstance instance2 = new ConsensusInstance(2, LogEntryState.KNOWN, 3,
                new byte[] {1});

        final PrepareOK prepareOk = new PrepareOK(3,
                new ConsensusInstance[] {instance0, instance1, instance2});

        dispatcher.dispatch(new Runnable() {
            public void run() {
                proposer.onPrepareOK(prepareOk, 1);
            }
        });
        dispatcher.execute();

        verify(paxos).decide(0);
        assertEquals(LogEntryState.KNOWN, storage.getLog().getInstance(0).getState());
        assertEquals(instance1, storage.getLog().getInstance(1));
        assertEquals(instance2, storage.getLog().getInstance(2));
    }

    @Test
    public void shouldFillUnknownInstancesWithNoOpAfterPrepareIsFinished() {
        storage.getLog().getInstance(2).setValue(1, new byte[] {1});
        storage.getLog().getInstance(2).setDecided();
        storage.updateFirstUncommitted();

        Proposer proposer = new ProposerImpl(paxos, network, failureDetector, storage,
                CrashModel.CrashStop);
        prepare(proposer);
        assertEquals(ProposerState.PREPARED, proposer.getState());

        // propose NoOp for instance 0 and 1
        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(3)).sendMessage(messageArgument.capture(), any(BitSet.class));

        ByteBuffer noOperationBuffer = ByteBuffer.allocate(4 + Request.NOP.byteSize());
        noOperationBuffer.putInt(1);
        Request.NOP.writeTo(noOperationBuffer);

        Propose propose0 = (Propose) messageArgument.getAllValues().get(1);
        Propose propose1 = (Propose) messageArgument.getAllValues().get(2);
        assertArrayEquals(noOperationBuffer.array(), propose0.getValue());
        assertEquals(0, propose0.getInstanceId());
        assertArrayEquals(noOperationBuffer.array(), propose1.getValue());
        assertEquals(1, propose1.getInstanceId());
    }

    @Test
    public void shouldProposeKnownInstancesAfterPrepareIsFinished() {
        storage.getLog().getInstance(0).setValue(1, new byte[] {1, 2});
        storage.getLog().getInstance(1).setValue(1, new byte[] {1, 2, 3});
        storage.getLog().getInstance(2).setValue(1, new byte[] {1});
        storage.getLog().getInstance(2).setDecided();
        storage.updateFirstUncommitted();

        Proposer proposer = new ProposerImpl(paxos, network, failureDetector, storage,
                CrashModel.CrashStop);
        prepare(proposer);

        // propose NoOp for instance 0 and 1
        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(3)).sendMessage(messageArgument.capture(), any(BitSet.class));

        Propose propose0 = (Propose) messageArgument.getAllValues().get(1);
        Propose propose1 = (Propose) messageArgument.getAllValues().get(2);

        assertArrayEquals(new byte[] {1, 2}, propose0.getValue());
        assertEquals(0, propose0.getInstanceId());
        assertArrayEquals(new byte[] {1, 2, 3}, propose1.getValue());
        assertEquals(1, propose1.getInstanceId());
    }

    @Test
    public void shouldProposeNewRequests() {
        final Proposer proposer = new ProposerImpl(paxos, network, failureDetector, storage,
                CrashModel.CrashStop);
        prepare(proposer);

        final Request request = new Request(new RequestId(1, 1), new byte[] {1, 2, 3});

        dispatcher.dispatch(new Runnable() {
            public void run() {
                proposer.enqueueRequest(request);
            }
        });
        dispatcher.execute();
        dispatcher.advanceTime(ProcessDescriptor.getInstance().maxBatchDelay);
        dispatcher.execute();

        ArgumentCaptor<Message> messageArgument = ArgumentCaptor.forClass(Message.class);
        verify(network, times(2)).sendMessage(messageArgument.capture(), any(BitSet.class));
        Propose propose = (Propose) messageArgument.getAllValues().get(1);
        assertEquals(0, propose.getInstanceId());
        ByteBuffer byteBuffer = ByteBuffer.allocate(request.byteSize() + 4);
        byteBuffer.putInt(1);
        request.writeTo(byteBuffer);
        assertArrayEquals(byteBuffer.array(), propose.getValue());
    }

    @Test
    public void shouldRejectPrepareOkWithOldEpochNumber() {
        storage.setEpoch(new long[] {0, 0, 0});
        final Proposer proposer = new ProposerImpl(paxos, network, failureDetector, storage,
                CrashModel.EpochSS);
        dispatcher.dispatch(new Runnable() {
            public void run() {
                storage.setView(5);
                proposer.prepareNextView();
                PrepareOK prepareOk1 = new PrepareOK(6,
                        new ConsensusInstance[] {}, new long[] {1, 1, 1});
                proposer.onPrepareOK(prepareOk1, 1);

                PrepareOK prepareOk2 = new PrepareOK(6,
                        new ConsensusInstance[] {}, new long[] {1, 2, 1});
                proposer.onPrepareOK(prepareOk2, 2);
            }
        });
        dispatcher.execute();
        assertEquals(ProposerState.PREPARING, proposer.getState());

        dispatcher.dispatch(new Runnable() {
            public void run() {
                PrepareOK prepareOk = new PrepareOK(6,
                        new ConsensusInstance[] {}, new long[] {1, 2, 1});
                proposer.onPrepareOK(prepareOk, 1);
            }
        });
        dispatcher.execute();
        assertEquals(ProposerState.PREPARED, proposer.getState());

        assertArrayEquals(new long[] {1, 2, 1}, storage.getEpoch());
    }

    private void prepare(final Proposer proposer) {
        storage.setView(5);
        dispatcher.dispatch(new Runnable() {
            public void run() {
                proposer.prepareNextView();

                PrepareOK prepareOk = new PrepareOK(6, new ConsensusInstance[] {});

                proposer.onPrepareOK(prepareOk, 1);
                proposer.onPrepareOK(prepareOk, 2);
            }
        });
        dispatcher.execute();
    }
}

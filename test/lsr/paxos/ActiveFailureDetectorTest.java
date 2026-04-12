package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.AliveReply;
import lsr.paxos.messages.Message;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

public class ActiveFailureDetectorTest {
    private ActiveFailureDetector failureDetector;
    private StubNetwork network;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        Storage storage = new InMemoryStorage();
        network = new StubNetwork();
        failureDetector = new ActiveFailureDetector(new FailureDetector.FailureDetectorListener() {
            @Override
            public void suspect(int view) {
            }
        }, network, storage);
    }

    @Test
    public void shouldAllowRuntimeTimeoutOverridesAndRestoreDefaults() {
        assertEquals(1000, failureDetector.getDefaultSuspectTimeout());
        assertEquals(500, failureDetector.getDefaultSendTimeout());
        assertEquals(1000, failureDetector.getSuspectTimeout());
        assertEquals(500, failureDetector.getSendTimeout());

        failureDetector.setSuspectTimeout(250);
        failureDetector.setSendTimeout(125);

        assertEquals(250, failureDetector.getSuspectTimeout());
        assertEquals(125, failureDetector.getSendTimeout());
        assertEquals(1000, failureDetector.getDefaultSuspectTimeout());
        assertEquals(500, failureDetector.getDefaultSendTimeout());

        failureDetector.restoreDefaultTimeouts();

        assertEquals(1000, failureDetector.getSuspectTimeout());
        assertEquals(500, failureDetector.getSendTimeout());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonPositiveSuspectTimeout() {
        failureDetector.setSuspectTimeout(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonPositiveSendTimeout() {
        failureDetector.setSendTimeout(-1);
    }

    @Test
    public void shouldSendAliveReplyWhenFollowerReceivesAlive() throws Exception {
        int view = 1;
        int leader = view % 3;
        int logNextId = 10;
        long heartbeatId = 123L;

        setFailureDetectorView(failureDetector, view);

        invokeOnMessageReceived(failureDetector, new Alive(view, logNextId, heartbeatId), leader);

        assertEquals(1, network.unicastCount);
        assertEquals(leader, network.lastDestination);
        assertTrue(network.lastUnicastMessage instanceof AliveReply);
        AliveReply reply = (AliveReply) network.lastUnicastMessage;
        assertEquals(view, reply.getView());
        assertEquals(heartbeatId, reply.getHeartbeatId());
        assertEquals(failureDetector.getSuspectTimeout() / 2, reply.getHeartbeatInterval());
    }

    @Test
    public void shouldClampFallbackHeartbeatIntervalToPositive() throws Exception {
        int view = 1;
        int leader = view % 3;
        int logNextId = 10;
        long heartbeatId = 123L;

        failureDetector.setSuspectTimeout(1);
        setFailureDetectorView(failureDetector, view);

        invokeOnMessageReceived(failureDetector, new Alive(view, logNextId, heartbeatId), leader);

        AliveReply reply = (AliveReply) network.lastUnicastMessage;
        assertEquals(1, reply.getHeartbeatInterval());
    }

    @Test
    public void shouldMeasureRttWhenLeaderReceivesAliveReply() throws Exception {
        long heartbeatId = 42L;
        long sentTs = ActiveFailureDetector.getTime() - 30;

        Method trackHeartbeat = ActiveFailureDetector.class.getDeclaredMethod(
                "trackHeartbeatSendTime", long.class, long.class);
        trackHeartbeat.setAccessible(true);
        trackHeartbeat.invoke(failureDetector, heartbeatId, sentTs);

        int suggestedHeartbeatInterval = 220;
        invokeOnMessageReceived(failureDetector,
                new AliveReply(0, heartbeatId, suggestedHeartbeatInterval), 1);

        long rtt = failureDetector.getLastRttForReplica(1);
        long oneWayDelay = failureDetector.getLastOneWayDelayForReplica(1);
        assertTrue(rtt >= 0);
        assertEquals(rtt / 2, oneWayDelay);
        assertEquals(suggestedHeartbeatInterval, failureDetector.getSendTimeout());
    }

    @Test
    public void shouldIgnoreOutOfRangeHeartbeatIntervalFeedback() throws Exception {
        long heartbeatId = 100L;
        long sentTs = ActiveFailureDetector.getTime() - 30;

        Method trackHeartbeat = ActiveFailureDetector.class.getDeclaredMethod(
                "trackHeartbeatSendTime", long.class, long.class);
        trackHeartbeat.setAccessible(true);
        trackHeartbeat.invoke(failureDetector, heartbeatId, sentTs);

        int originalSendTimeout = failureDetector.getSendTimeout();
        invokeOnMessageReceived(failureDetector,
                new AliveReply(0, heartbeatId, (long) Integer.MAX_VALUE + 1), 1);

        assertEquals(originalSendTimeout, failureDetector.getSendTimeout());
    }

    @Test
    public void shouldTrackFollowerObservationWindowsAndResetOnViewChange() throws Exception {
        int view = 1;
        int leader = view % 3;
        int maxWindowSize = lsr.common.ProcessDescriptor.processDescriptor.dynatuneMaxListSize;

        setFailureDetectorView(failureDetector, view);
        for (int i = 0; i < maxWindowSize + 5; i++) {
            invokeOnMessageReceived(failureDetector,
                    new Alive(view, 10, i, 30 + i, 100), leader);
        }

        assertEquals(maxWindowSize, failureDetector.getObservedRttCount());
        assertEquals(maxWindowSize, failureDetector.getObservedHeartbeatIdCount());
        assertEquals(30 + maxWindowSize + 4, failureDetector.getLastObservedRtt());

        invokeViewChanged(failureDetector, view + 1);
        assertEquals(0, failureDetector.getObservedRttCount());
        assertEquals(0, failureDetector.getObservedHeartbeatIdCount());
    }

    @Test
    public void shouldIgnoreStaleAliveFromSameLeaderId() throws Exception {
        int view = 1;
        int staleViewWithSameLeaderId = view + 3;
        int leader = view % 3;

        setFailureDetectorView(failureDetector, view);
        invokeOnMessageReceived(failureDetector,
                new Alive(staleViewWithSameLeaderId, 10, 1, 50, 100), leader);

        assertEquals(0, network.unicastCount);
        assertEquals(0, failureDetector.getObservedRttCount());
        assertEquals(0, failureDetector.getObservedHeartbeatIdCount());
    }

    @Test
    public void shouldIgnoreNonMonotonicHeartbeatIds() throws Exception {
        int view = 1;
        int leader = view % 3;

        setFailureDetectorView(failureDetector, view);
        invokeOnMessageReceived(failureDetector, new Alive(view, 10, 5, 10, 100), leader);
        invokeOnMessageReceived(failureDetector, new Alive(view, 10, 4, 12, 100), leader);
        invokeOnMessageReceived(failureDetector, new Alive(view, 10, 6, 14, 100), leader);

        assertEquals(2, failureDetector.getObservedHeartbeatIdCount());
    }

    @Test
    public void shouldComputeEtAndHeartbeatIntervalFromObservations() throws Exception {
        initializeProcessDescriptorWithDynatune(3, 0, true, 0.0, 0.5, 3, 10);
        Storage storage = new InMemoryStorage();
        network = new StubNetwork();
        failureDetector = new ActiveFailureDetector(new FailureDetector.FailureDetectorListener() {
            @Override
            public void suspect(int view) {
            }
        }, network, storage);

        int view = 1;
        int leader = view % 3;
        setFailureDetectorView(failureDetector, view);

        invokeOnMessageReceived(failureDetector, new Alive(view, 10, 1, 10, 100), leader);
        invokeOnMessageReceived(failureDetector, new Alive(view, 10, 2, 14, 100), leader);
        invokeOnMessageReceived(failureDetector, new Alive(view, 10, 4, 18, 100), leader);

        AliveReply reply = (AliveReply) network.lastUnicastMessage;
        assertEquals(14, failureDetector.getSuspectTimeout());
        assertEquals(4, reply.getHeartbeatInterval());
        assertEquals(14, failureDetector.getLastComputedEt());
        assertEquals(4, failureDetector.getLastSuggestedHeartbeatInterval());
    }

    private static void invokeOnMessageReceived(ActiveFailureDetector fd, Message message, int sender)
            throws Exception {
        Field innerListenerField = ActiveFailureDetector.class.getDeclaredField("innerListener");
        innerListenerField.setAccessible(true);
        MessageHandler listener = (MessageHandler) innerListenerField.get(fd);
        listener.onMessageReceived(message, sender);
    }

    private static void setFailureDetectorView(ActiveFailureDetector detector, int view)
            throws Exception {
        Field viewField = ActiveFailureDetector.class.getDeclaredField("view");
        viewField.setAccessible(true);
        viewField.setInt(detector, view);
    }

    private static void invokeViewChanged(ActiveFailureDetector detector, int newView)
            throws Exception {
        Field listenerField = ActiveFailureDetector.class.getDeclaredField("viewCahngeListener");
        listenerField.setAccessible(true);
        Storage.ViewChangeListener listener = (Storage.ViewChangeListener) listenerField.get(detector);
        listener.viewChanged(newView, newView % 3);
    }

    private static void initializeProcessDescriptorWithDynatune(int numReplicas, int localId,
                                                                boolean enabled, double safetyFactor,
                                                                double heartbeatProbability,
                                                                int minListSize, int maxListSize)
            throws Exception {
        Properties properties = new Properties();
        for (int i = 0; i < numReplicas; i++) {
            properties.setProperty("process." + i, "localhost:" + (2000 + i) + ":" + (3000 + i));
        }
        properties.setProperty(ProcessDescriptor.DYNATUNE_ENABLED, Boolean.toString(enabled));
        properties.setProperty(ProcessDescriptor.DYNATUNE_SAFETY_FACTOR,
                Double.toString(safetyFactor));
        properties.setProperty(ProcessDescriptor.DYNATUNE_HEARTBEAT_PROBABILITY,
                Double.toString(heartbeatProbability));
        properties.setProperty(ProcessDescriptor.DYNATUNE_MIN_LIST_SIZE,
                Integer.toString(minListSize));
        properties.setProperty(ProcessDescriptor.DYNATUNE_MAX_LIST_SIZE,
                Integer.toString(maxListSize));

        File tempConfig = File.createTempFile("jpaxos-dynatune-test", ".properties");
        tempConfig.deleteOnExit();
        FileOutputStream out = new FileOutputStream(tempConfig);
        try {
            properties.store(out, "dynatune test config");
        } finally {
            out.close();
        }
        Configuration configuration = new Configuration(tempConfig.getAbsolutePath());
        ProcessDescriptor.initialize(configuration, localId);
    }

    private static class StubNetwork extends Network {
        private Message lastUnicastMessage;
        private int lastDestination = -1;
        private int unicastCount;

        @Override
        protected void send(Message message, int destination) {
            this.lastUnicastMessage = message;
            this.lastDestination = destination;
            this.unicastCount++;
        }

        @Override
        protected void send(Message message, BitSet destinations) {
        }

        @Override
        public void start() {
        }
    }
}

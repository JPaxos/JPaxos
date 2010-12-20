package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lsr.paxos.storage.ConsensusInstance;

import org.junit.Before;
import org.junit.Test;

public class CatchUpResponseTest extends AbstractMessageTestCase<CatchUpResponse> {
    private int view = 12;
    private long requestTime = 574351;
    private CatchUpResponse catchUpResponse;
    private List<ConsensusInstance> instances;

    @Before
    public void setUp() {
        instances = new ArrayList<ConsensusInstance>();
        instances.add(new ConsensusInstance(0));
        instances.get(0).setValue(4, new byte[] {1, 2, 3});
        instances.add(new ConsensusInstance(1));
        instances.get(0).setValue(5, new byte[] {1, 4, 3});
        instances.add(new ConsensusInstance(2));
        instances.get(0).setValue(6, new byte[] {6, 9, 2});

        catchUpResponse = new CatchUpResponse(view, requestTime, instances);
    }

    @Test
    public void shouldInitializeFieldsInConstructor() {
        assertEquals(view, catchUpResponse.getView());
        assertEquals(requestTime, catchUpResponse.getRequestTime());
        assertEquals(instances, catchUpResponse.getDecided());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(catchUpResponse);

        byte[] bytes = catchUpResponse.toByteArray();

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        CatchUpResponse deserializedPrepare = new CatchUpResponse(dis);

        assertEquals(MessageType.CatchUpResponse, type);
        compare(catchUpResponse, deserializedPrepare);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.CatchUpResponse, catchUpResponse.getType());
    }

    protected void compare(CatchUpResponse expected, CatchUpResponse actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());

        assertEquals(expected.getRequestTime(), actual.getRequestTime());
        assertEquals(expected.getDecided(), actual.getDecided());
    }
}

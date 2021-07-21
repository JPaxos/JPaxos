package lsr.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;

import org.junit.Test;

import lsr.common.ClientCommand.CommandType;

public class ClientRequest1Test {
    
    @Test
    public void shouldInitialize() {
        RequestId requestId = new RequestId(1, 1);
        ClientRequest request = new ClientRequest(requestId, new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);

        assertEquals(CommandType.REQUEST, command.getCommandType());
        assertEquals(request, command.getRequest());
    }

    @Test
    public void shouldSerializeAndDeserialize() {
        RequestId requestId = new RequestId(1, 1);
        ClientRequest request = new ClientRequest(requestId, new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);

        ByteBuffer byteBuffer = ByteBuffer.allocate(command.byteSize());
        command.writeTo(byteBuffer);

        assertFalse(byteBuffer.hasRemaining());

        byteBuffer.rewind();

        ClientCommand actual = new ClientCommand(byteBuffer);
        assertEquals(command.getCommandType(), actual.getCommandType());
        assertEquals(command.getRequest(), actual.getRequest());
    }
}

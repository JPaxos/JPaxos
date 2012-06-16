package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.common.ClientRequest;

public final class ForwardedRequest extends Message {
    private static final long serialVersionUID = 1L;
    public final ClientRequest request;

    protected ForwardedRequest(DataInputStream input) throws IOException {
        super(input);
        request = ClientRequest.create(input);
    }

    public ForwardedRequest(ClientRequest request) {
        super(-1);
        this.request = request;
    }

    @Override
    public MessageType getType() {
        return MessageType.ForwardedRequest;
    }

    @Override
    protected void write(ByteBuffer bb) {
        request.writeTo(bb);
    }

    public int byteSize() {
        return super.byteSize() + request.byteSize();
    }

    public String toString() {
        return "ForwardedRequest(" + super.toString() + ", " + request + ")";
    }
}

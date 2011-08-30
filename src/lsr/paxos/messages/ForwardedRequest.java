package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.common.Request;

public final class ForwardedRequest extends Message {
    private static final long serialVersionUID = 1L;
    public final Request request;

    protected ForwardedRequest(DataInputStream input) throws IOException {
        super(input);
        request = Request.create(input);
    }
    
    public ForwardedRequest(Request request) {
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

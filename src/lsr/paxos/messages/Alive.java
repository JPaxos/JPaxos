package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Alive extends Message {
    private static final long serialVersionUID = 1L;
    /**
     * LogSize is the size of log (== the highest started instanceID) of the
     * leader
     */
    private int logSize;

    public Alive(int view, int logSize) {
        super(view);
        this.logSize = logSize;
    }

    public Alive(DataInputStream input) throws IOException {
        super(input);
        logSize = input.readInt();
    }

    public int getLogSize() {
        return logSize;
    }

    public MessageType getType() {
        return MessageType.Alive;
    }

    // protected void write(DataOutputStream os) throws IOException {
    // os.writeInt(_logSize);
    // }

    public int byteSize() {
        return super.byteSize() + 4;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + logSize;
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Alive other = (Alive) obj;
        if (logSize != other.logSize)
            return false;
        return true;
    }

    public String toString() {
        return "ALIVE (" + super.toString() + ", logsize: " + logSize + ")";
    }

    protected void write(ByteBuffer bb) throws IOException {
        bb.putInt(logSize);
    }
}

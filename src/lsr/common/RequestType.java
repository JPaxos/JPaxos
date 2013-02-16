package lsr.common;

import java.nio.ByteBuffer;

public interface RequestType {

    void writeTo(ByteBuffer bb);

    int byteSize();

}

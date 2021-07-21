package lsr.common.nio;

import java.nio.channels.SelectionKey;

public interface ReadWriteHandler {
    void handleRead(SelectionKey key);

    void handleWrite(SelectionKey key);
}

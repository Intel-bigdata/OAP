package org.apache.spark.network.pmof;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ShuffleBufferInputStream extends InputStream {

    private final ShuffleBuffer shuffleBuffer;
    private final ByteBuffer buf;

    public ShuffleBufferInputStream(ShuffleBuffer shuffleBuffer) {
        this.shuffleBuffer = shuffleBuffer;
        this.buf = shuffleBuffer.nioByteBuffer();
    }

    public int read() {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }

    public void close() {
        shuffleBuffer.close();
    }

    public int available() {
        return buf.remaining();
    }
}

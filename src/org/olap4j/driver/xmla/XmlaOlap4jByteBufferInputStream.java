package org.olap4j.driver.xmla;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class XmlaOlap4jByteBufferInputStream extends InputStream {
    private ByteBuffer byteBuffer;

    /** Creates an uninitialized stream that cannot be used until {@link #setByteBuffer(ByteBuffer)} is called. */
    public XmlaOlap4jByteBufferInputStream (ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer () {
        return byteBuffer;
    }

    public void setByteBuffer (ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public int read () throws IOException {
        if (!byteBuffer.hasRemaining()) return -1;
        return byteBuffer.get() & 0xFF;
    }

    public int read (byte[] bytes, int offset, int length) throws IOException {
        if (length == 0) return 0;
        int count = Math.min(byteBuffer.remaining(), length);
        if (count == 0) return -1;
        byteBuffer.get(bytes, offset, count);
        return count;
    }

    public int available () throws IOException {
        return byteBuffer.remaining();
    }
}

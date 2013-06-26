import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedFile implements Closeable {
    public static final long DEFAULT_FILE_SIZE = 1*1024*1024*1024;
    public static final int HEADER_SIZE = 6;
    public static final int FOOTER_SIZE = 2;
    public static final int OVERHEAD_SIZE = HEADER_SIZE + FOOTER_SIZE;

    private static final short HEADER_MAGIC = (short)0xAAAA;
    private static final short FOOTER_MAGIC = (short)0xEEEE;

    private final RandomAccessFile randomAccessFile;
    private final MappedByteBuffer mappedByteBuffer;

    public static int estimateFileSize(int numMessages, int messageSize) {
        return ((messageSize + OVERHEAD_SIZE) * numMessages) + OVERHEAD_SIZE;
    }

    MappedFile(File file, long requiredFileLength) throws IOException {
        if (requiredFileLength <= OVERHEAD_SIZE * 2) {
            throw new IOException("file size too small");
        }
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        final long fileLength = randomAccessFile.length();
        if (fileLength == 0) {
            randomAccessFile.setLength(requiredFileLength);
            randomAccessFile.getChannel().force(true);
        } else if (fileLength != requiredFileLength) {
            throw new IOException(
                    String.format("%s bad file size %s != %s", file, fileLength, requiredFileLength)
            );
        }
        // TODO: perhaps support files larger than
        mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, requiredFileLength);
    }

    public boolean offer(ByteBuffer buffer) {
        int length = buffer.remaining();
        if (remaining() >= length) {
            mappedByteBuffer.putInt(length);
            //System.currentTimeMillis();
            mappedByteBuffer.putShort(HEADER_MAGIC);
            mappedByteBuffer.put(buffer);
            mappedByteBuffer.putShort(FOOTER_MAGIC);
            //mappedByteBuffer.force();
            return true;
        }
        return false;
    }

    public ByteBuffer poll() {
        mappedByteBuffer.mark();
        int size = mappedByteBuffer.getInt();
        short magic = mappedByteBuffer.getShort();
        if (magic == HEADER_MAGIC) {
            ByteBuffer tmp = mappedByteBuffer.slice();
            mappedByteBuffer.position(mappedByteBuffer.position() + size);
            short endMagic = mappedByteBuffer.getShort();
            if (endMagic == FOOTER_MAGIC) {
                tmp.limit(size);
                return tmp;
            }
        }
        mappedByteBuffer.reset();
        return null;
    }

    public ByteBuffer take() throws InterruptedException {
        ByteBuffer tmp;
        while ((tmp = poll()) == null) {
            Thread.yield();
            // TODO: should start sleeping after a while
        }
        return tmp;
    }

    public int remaining() {
        return mappedByteBuffer.remaining() - (OVERHEAD_SIZE * 2);
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }
}

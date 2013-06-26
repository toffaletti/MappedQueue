import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Checksum;

public class MappedFile implements Closeable {
    public static final long DEFAULT_FILE_SIZE = 1 * 1024 * 1024 * 1024;
    public static final long OVERHEAD_SIZE = 8L;
    private static final int MAGIC_FOOTER = 0xd34db33f;

    private final RandomAccessFile randomAccessFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final Checksum checksum;

    public static int estimateFileSize(int numMessages, int messageSize) {
        long fileSize = ((messageSize + OVERHEAD_SIZE) * numMessages) + OVERHEAD_SIZE;
        assert fileSize < Integer.MAX_VALUE;
        return (int) fileSize;
    }

    MappedFile(File file, long requiredFileLength) throws IOException {
        this(file, requiredFileLength, new PureJavaCrc32());
    }

    MappedFile(File file, long requiredFileLength, Checksum checksum) throws IOException {
        this.checksum = checksum;
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
        mappedByteBuffer.load();
    }

    public boolean offer(ByteBuffer buffer) {
        int length = buffer.remaining();
        if (remaining() >= length) {
            final int dataChecksum;
            if (checksum != null) {
                checksum.reset();

                buffer.mark();
                for (int i = 0; i < length; ++i) {
                    checksum.update(buffer.get());
                }
                buffer.reset();
                dataChecksum = (int) checksum.getValue();
            } else {
                dataChecksum = MAGIC_FOOTER;
            }

            mappedByteBuffer.putInt(length);
            mappedByteBuffer.put(buffer);
            mappedByteBuffer.putInt(dataChecksum);
            //mappedByteBuffer.force();
            return true;
        }
        return false;
    }

    public ByteBuffer poll() {
        mappedByteBuffer.mark();
        int length = mappedByteBuffer.getInt();
        if (length > 0 && length < Integer.MAX_VALUE) {
            ByteBuffer tmp = mappedByteBuffer.slice();
            mappedByteBuffer.position(mappedByteBuffer.position() + length);
            final int expectedChecksum = mappedByteBuffer.getInt();

            // calculate checksum of data in slice
            final int dataChecksum;
            if (checksum != null) {
                checksum.reset();
                tmp.mark();
                for (int i = 0; i < length; ++i) {
                    checksum.update(tmp.get());
                }
                tmp.reset();
                dataChecksum = (int) checksum.getValue();
            } else {
                dataChecksum = MAGIC_FOOTER;
            }

            if (expectedChecksum == dataChecksum) {
                tmp.limit(length);
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
        return (int) (mappedByteBuffer.remaining() - (OVERHEAD_SIZE * 2));
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }
}

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
    private static final int EOF_FOOTER = 0xfe0fe0ff;

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
        assert length > 0;
        if (remaining() >= length) {
            final int dataChecksum = checksum(buffer);

            mappedByteBuffer.putInt(length);
            buffer.mark();
            mappedByteBuffer.put(buffer);
            buffer.reset();
            mappedByteBuffer.putInt(dataChecksum);
            //mappedByteBuffer.force();
            return true;
        } else {
            // write eof mark
            mappedByteBuffer.mark();
            finish();
            mappedByteBuffer.reset();
        }
        return false;
    }

    public void finish() {
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(EOF_FOOTER);
    }

    public ByteBuffer poll() {
        mappedByteBuffer.mark();
        int length = mappedByteBuffer.getInt();
        if (length > 0 && length < Integer.MAX_VALUE) {
            ByteBuffer tmp = mappedByteBuffer.slice();
            tmp.limit(length);
            mappedByteBuffer.position(mappedByteBuffer.position() + length);
            final int expectedChecksum = mappedByteBuffer.getInt();

            // calculate checksum of data in slice
            final int dataChecksum = checksum(tmp);

            if (expectedChecksum == dataChecksum) {
                return tmp;
            }
        } else if (length == 0) {
            // avoid spinning on eof by returning empty ByteBuffer
            final int footer = mappedByteBuffer.getInt();
            if (footer == EOF_FOOTER) {
                mappedByteBuffer.reset();
                return ByteBuffer.allocate(0);
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

    private int checksum(ByteBuffer data) {
        final int dataChecksum;
        if (checksum != null) {
            checksum.reset();
            data.mark();
            byte[] buf = new byte[1024];
            while (data.remaining() > 0) {
                int readSize = Math.min(1024, data.remaining());
                data.get(buf, 0, readSize);
                checksum.update(buf, 0, readSize);
            }
            data.reset();
            dataChecksum = (int) checksum.getValue();
        } else {
            dataChecksum = MAGIC_FOOTER;
        }
        return dataChecksum;
    }
}

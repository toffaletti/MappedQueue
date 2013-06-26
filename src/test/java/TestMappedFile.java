import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.zip.Checksum;

public class TestMappedFile {

    @Test
    public void offerOne() throws Exception {
        final File file = File.createTempFile("mapped", "test");
        file.deleteOnExit();
        ByteBuffer offerData = ByteBuffer.wrap("hello".getBytes());
        {
            final MappedFile mappedFile = new MappedFile(file, 1024 * 1024);
            Assert.assertTrue(mappedFile.offer(offerData));
        }

        {
            final MappedFile mappedFile = new MappedFile(file, 1024 * 1024);
            ByteBuffer data = mappedFile.take();
            offerData.flip();
            Assert.assertEquals(offerData, data);
        }
    }

    private void writeMessages(long iterations, ByteBuffer data, File file, long fileSize, Checksum checksum) throws IOException {
        final MappedFile mappedFile = new MappedFile(file, fileSize, checksum);
        for (int i = 0; i < iterations; ++i) {
            if (!mappedFile.offer(data)) {
                Assert.fail();
            }
            data.flip();
        }
        mappedFile.close();
    }

    private void readMessages(long iterations, File file, long fileSize, Checksum checksum) throws IOException, InterruptedException {
        final MappedFile mappedFile = new MappedFile(file, fileSize, checksum);
        for (int i = 0; i < iterations; ++i) {
            mappedFile.take();
        }
        mappedFile.close();
    }

    public void largeMessages(String name, final Checksum writeChecksum, final Checksum readChecksum) throws IOException, ExecutionException, InterruptedException {
        final File file = File.createTempFile("mapped", "test");
        file.deleteOnExit();
        final int iterations = 1000;
        final int msgSize = 1024 * 1024;
        final ByteBuffer data = ByteBuffer.allocate(msgSize);
        final long fileSize = MappedFile.estimateFileSize(iterations, msgSize);
        final ExecutorService executorService = Executors.newCachedThreadPool();
        Future<Long> writeFuture = executorService.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                long start = System.nanoTime();
                writeMessages(iterations, data, file, fileSize, writeChecksum);
                long stop = System.nanoTime();
                return stop - start;
            }
        });

        Future<Long> readFuture = executorService.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                long start = System.nanoTime();
                readMessages(iterations, file, fileSize, readChecksum);
                long stop = System.nanoTime();
                return stop - start;
            }
        });

        long start = System.nanoTime();
        long writeElapsed = writeFuture.get();
        long readElapsed = readFuture.get();
        long stop = System.nanoTime();

        System.out.printf("%s elapsed total: %s write %s read %s%n",
                name,
                TimeUnit.NANOSECONDS.toMillis(stop - start),
                TimeUnit.NANOSECONDS.toMillis(writeElapsed),
                TimeUnit.NANOSECONDS.toMillis(readElapsed)
        );
    }

    @Test
    public void largeMessagesWithChecksum() throws Exception {
        largeMessages("with checksum", new PureJavaCrc32(), new PureJavaCrc32());
    }

    @Test
    public void largeMessagesNoChecksum() throws Exception {
        largeMessages("no checksum", null, null);
    }

    @Test(timeout = 1000)
    public void timing() throws Exception {
        final int ITERATIONS = 100000;
        final int FILE_SIZE = MappedFile.estimateFileSize(ITERATIONS, 8);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final File file = File.createTempFile("mapped", "test");
        file.deleteOnExit();
        final Future<Integer> future = executorService.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                long worstLatency = Long.MIN_VALUE;
                long bestLatency = Long.MAX_VALUE;
                int count = 0;
                int misses = 0;
                try {
                    final MappedFile mappedFile = new MappedFile(file, FILE_SIZE);
                    while (count < ITERATIONS) {
                        ByteBuffer buf;
                        while ((buf = mappedFile.poll()) == null) {
                            Thread.yield();
                            ++misses;
                        }
                        long now = System.nanoTime();
                        long sendTime = buf.getLong();
                        long elapsed = now - sendTime;
                        if (elapsed > worstLatency) {
                            worstLatency = elapsed;
                        }
                        if (elapsed < bestLatency) {
                            bestLatency = elapsed;
                        }
                        ++count;
                    }
                    System.out.printf("latency best %s worst %s%n",
                            bestLatency,
                            worstLatency);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return misses;
            }
        });
        long start = System.nanoTime();
        final MappedFile mappedFile = new MappedFile(file, FILE_SIZE);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        for (int i = 0; i < ITERATIONS; ++i) {
            buffer.clear();
            buffer.putLong(System.nanoTime());
            buffer.flip();
            mappedFile.offer(buffer);
        }
        long totalNanos = System.nanoTime() - start;
        System.out.printf("misses: %s%n", future.get());
        System.out.printf("total millis: %s avg nanos: %s%n",
                TimeUnit.NANOSECONDS.toMillis(totalNanos),
                totalNanos / ITERATIONS);
        mappedFile.close();
    }

    @Test(expected = IOException.class)
    public void badFileSize() throws Exception {
        final File file = File.createTempFile("mapped", "test");
        file.deleteOnExit();
        {
            final MappedFile mappedFile = new MappedFile(file, 1000);
            mappedFile.close();
        }

        {
            final MappedFile mappedFile = new MappedFile(file, 2000);
            mappedFile.close();
        }
    }

    @Test(expected = IOException.class)
    public void tooSmall() throws Exception {
        final File file = File.createTempFile("mapped", "test");
        file.deleteOnExit();
        final MappedFile mappedFile = new MappedFile(file, 1);
        mappedFile.close();
    }
}

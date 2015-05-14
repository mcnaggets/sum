package com.whendu.sample;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class Sum {

    public static final int BYTES_PER_NUMBER = 4;

    public static final Path DEFAULT_PATH = Paths.get(System.getProperty("user.dir") + "\\sample.txt");
    public static final int THREADS = 4;
    public static final int NUMBERS_TO_READ = 10_000;
    public static final int CAPACITY = BYTES_PER_NUMBER * NUMBERS_TO_READ;

    private static Path path;

    public static void main(String[] args) throws IOException, InterruptedException {
        initPath(args);
        System.out.println("Calculation started");
        System.out.printf("Number of threads %d. Buffer capacity %d numbers\n", THREADS, NUMBERS_TO_READ);
        long time = System.currentTimeMillis();
        doMain();
        System.out.printf("Calculation done! Elapsed time %s ms\n", System.currentTimeMillis() - time);
    }

    private static void doMain() throws InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
        System.out.printf("Sum: %d\n", executorService.invokeAll(range(0, THREADS)
                .mapToObj(Sum::createSumCounter).collect(toList())).stream()
                .mapToLong(Sum::getALong).sum());
        executorService.shutdown();
    }

    private static void initPath(String[] args) throws IOException {
        if (args.length > 0) path = Paths.get(args[0]);
        else {
            System.out.printf("No file path specified. Default file with %d digits will be generated\n", 1_000_000);
            System.out.println("Generating numbers 1 2 3 4 5 6 7 8 ...");
            WriteBigFile.main(args);
            path = DEFAULT_PATH;
        }
    }

    private static SumCounter createSumCounter(int i) {
        try {
            final long size = Files.size(path);
            return new SumCounter(fileOffset(i, size), fileOffset(i + 1, size));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long fileOffset(int threadNumber, long fileSize) {
        final long offset = threadNumber * fileSize / THREADS;
        return offset - (offset % BYTES_PER_NUMBER);
    }

    private static Long getALong(Future<Long> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            return 0l;
        }
    }

    static class SumCounter implements Callable<Long> {

        private final long from;
        private final long to;

        public SumCounter(long from, long to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public Long call() throws Exception {
            long time = System.currentTimeMillis();

            long sum = 0;
            final ByteBuffer buffer = ByteBuffer.allocate(CAPACITY).order(LITTLE_ENDIAN);
            try (final SeekableByteChannel channel = Files.newByteChannel(path)) {
                channel.position(from);
                while (canRead(buffer, channel)) {
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        sum += buffer.getInt();
                    }
                    buffer.clear();
                }
            }

            System.out.printf("Thread %s is finished (sum: %d) within %s ms\n",
                    Thread.currentThread().getId(), sum, System.currentTimeMillis() - time);

            return sum;
        }

        private boolean canRead(ByteBuffer buffer, SeekableByteChannel channel) throws IOException {
            final long bytesLeft = to - channel.position();
            if (bytesLeft < CAPACITY) {
                buffer.limit((int) bytesLeft);
            }
            return channel.position() < to && channel.read(buffer) != -1;
        }

    }

}

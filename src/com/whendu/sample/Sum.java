package com.whendu.sample;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.*;

public class Sum {

    public static final int BYTES_PER_NUMBER = 4;

    public static final String FILE_PATH = "D:\\tmp\\sum\\examples\\1_000_000_000.txt";
    public static final Path PATH = Paths.get(FILE_PATH);
    public static final int THREADS = 3;
    public static final int NUMBERS_TO_READ = 10_000;
    public static final int CAPACITY = BYTES_PER_NUMBER * NUMBERS_TO_READ;

    public static void main(String[] args) throws IOException, InterruptedException {
        long time = System.currentTimeMillis();

        final long size = Files.size(PATH);
        final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
        double sum = executorService.invokeAll(
                range(0, THREADS).mapToObj(i -> createSumCounter(size, i))
                        .collect(toList())).stream().mapToLong(Sum::getALong).sum();
        executorService.shutdown();

        System.out.printf("Sum %.0f\n", sum);
        System.out.printf("Elapsed time %s ms\n", System.currentTimeMillis() - time);
    }

    private static SumCounter createSumCounter(long size, int i) {
        return new SumCounter(fileOffset(i, size), fileOffset(i + 1, size));
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
            try (final SeekableByteChannel channel = Files.newByteChannel(PATH)) {
                channel.position(from);
                while (canRead(buffer, channel)) {
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        sum += buffer.getInt();
                    }
                    buffer.clear();
                }
            }

            System.out.printf("Thread %s is finished (%d) within %s ms\n",
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

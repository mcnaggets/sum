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

public class Sum {

    public static final int BYTES_PER_NUMBER = 4;

    public static final String FILE_PATH = "D:\\tmp\\sum\\examples\\1_000_000_000.txt";
    public static final Path PATH = Paths.get(FILE_PATH);
    public static final int THREADS = 4;
    public static final int NUMBERS_TO_READ = 3_000;

    public static void main(String[] args) throws IOException, InterruptedException {
        long time = System.currentTimeMillis();

        final long size = Files.size(PATH);
        final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
        double sum = executorService.invokeAll(
                IntStream.range(0, THREADS).mapToObj(i -> createSumCounter(size, i))
                        .collect(toList())).stream().mapToDouble(Sum::getADouble).sum();
        executorService.shutdown();

        System.out.printf("Sum %.0f\n", sum);
        System.out.printf("Elapsed time %s ms\n", System.currentTimeMillis() - time);
    }

    private static SumCounter createSumCounter(long size, int i) {
        return new SumCounter(fileOffset(i, size), fileOffset(i + 1, size));
    }

    private static long fileOffset(int threadNumber, long fileSize) {
        return threadNumber * fileSize / THREADS;
    }

    private static Double getADouble(Future<Double> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            return 0d;
        }
    }

    static class SumCounter implements Callable<Double> {

        private final long from;
        private final long to;

        public SumCounter(long from, long to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public Double call() throws Exception {
            long time = System.currentTimeMillis();

            double sum = 0;
            final ByteBuffer buffer = ByteBuffer.allocate(BYTES_PER_NUMBER * NUMBERS_TO_READ).order(LITTLE_ENDIAN);
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
            System.out.printf("Thread %s is finished (%.0f) within %s ms\n",
                    Thread.currentThread().getId(), sum, System.currentTimeMillis() - time);
            return sum;
        }

        private boolean canRead(ByteBuffer buffer, SeekableByteChannel channel) throws IOException {
            return channel.position() < to && channel.read(buffer) != -1;
        }

    }

}

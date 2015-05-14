package com.whendu.sample;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class Sum {

    public static final String FILE_PATH = "D:\\tmp\\sum\\examples\\100_000_000.txt";
    public static final Path PATH = Paths.get(FILE_PATH);

    public static void main(String[] args) throws IOException, InterruptedException {

        double time = System.currentTimeMillis();


        final long size = Files.size(PATH);
        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        double sum = executorService.invokeAll(Stream.of(
                new SumCounter(0, size / 4), new SumCounter(size / 4, size / 2), new SumCounter(size / 2, 3 * size / 4), new SumCounter(3 * size / 4, size)
//                new SumCounter(0, size / 2), new SumCounter(size / 2, size)
//                new SumCounter(0, size)
        ).collect(Collectors.toList())).stream().mapToDouble(Sum::getADouble).sum();
        executorService.shutdown();

        System.out.printf("Sum %.0f\n", sum);
        System.out.printf("Elapsed time %s ms", System.currentTimeMillis() - time);
    }

    private static Double getADouble(Future<Double> t) {
        try {
            return t.get();
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
            double sum = 0;
            final ByteBuffer buffer = ByteBuffer.allocate(4);
            try (final SeekableByteChannel channel = Files.newByteChannel(PATH)) {
                channel.position(from);
                while (canRead(buffer, channel)) {
                    buffer.flip();
                    sum += buffer.order(LITTLE_ENDIAN).getInt();
                    buffer.clear();
                }
            }
            return sum;
        }

        private boolean canRead(ByteBuffer buffer, SeekableByteChannel channel) throws IOException {
            return channel.position() < to && channel.read(buffer) != -1;
        }

    }

}

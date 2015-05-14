package com.whendu.sample;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.whendu.sample.Sum.DEFAULT_PATH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class WriteBigFile {

    private static Path path;

    public static void main(String[] args) throws IOException {
        long time = System.currentTimeMillis();
        initPath(args);
        System.out.println("Generation started.");
        doMain();
        System.out.printf("Generation done. Elapsed time: %d ms\n", System.currentTimeMillis() - time);
    }

    private static void doMain() throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
        try (final SeekableByteChannel channel = Files.newByteChannel(path, CREATE, WRITE)) {
            for (int i = 1; i <= 1_000_000; i++) {
                buffer.clear();
                buffer.putInt(i);
                buffer.flip();
                channel.write(buffer);
            }
        }
    }

    private static void initPath(String[] args) {
        if (args.length > 0) path = Paths.get(args[0]);
        else path = DEFAULT_PATH;
    }

}

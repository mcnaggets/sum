package com.whendu.sample;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class WriteBigFile {

    public static void main(String[] args) throws IOException {

        final String filePath = "D:\\tmp\\sum\\examples\\100_000_000.txt";

        double time = System.currentTimeMillis();

        final ByteBuffer buffer = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
        final Path path = Paths.get(filePath);
        try (final SeekableByteChannel channel = Files.newByteChannel(path, CREATE, WRITE)) {
            for (int i = 1; i <= 100_000_000; i++) {
                buffer.clear();
                buffer.putInt(i);
                buffer.flip();
                channel.write(buffer);
            }
        }

        System.out.println("Elapsed time in ms: " + (System.currentTimeMillis() - time));
    }


}

package com.whendu.sample;

import java.util.stream.LongStream;

public class JustCheck {

    public static void main(String[] args) {
        System.out.printf("Sum: %d\n", LongStream.range(1, 1_000_000 + 1).sum());
    }
}

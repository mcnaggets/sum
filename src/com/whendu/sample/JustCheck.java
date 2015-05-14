package com.whendu.sample;

import java.util.stream.IntStream;

public class JustCheck {

    public static void main(String[] args) {
        final double sum = IntStream.range(1, 1_001).mapToDouble(Double::valueOf).sum();
        System.out.printf("Sum %.0f\n", sum);
    }
}

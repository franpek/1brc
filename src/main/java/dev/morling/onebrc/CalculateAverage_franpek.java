/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;

/**
 * Changelog:
 * <p>
 * Approximate measures:
 * - Baseline:                                              190700 ms
 * - Use of a parallel stream:                              70000 ms
 * - ConcurrentSkipListMap instead of Tree map:             70000 ms
 * - No improvement by adding Collector.Characteristics.CONCURRENT nor eliminating the use of Measurement and ResultRow. (Maybe did something wrong)
 * - After studying and applying the @twobiers's solution:  16810ms
 * <p>
 * Quick launch with: jbang src/main/java/dev/morling/onebrc/CalculateAverage_franpek.java
 */
public class CalculateAverage_franpek {

    private static final String FILE = "./measurements.txt";

    private static class AverageCollector implements Collector<Measurement, double[], String> {
        @Override
        public Supplier<double[]> supplier() {
            // Function that creates a new array of doubles of size 4.
            // min, max, sum, count
            return () -> new double[4];
        }

        @Override
        public BiConsumer<double[], Measurement> accumulator() {
            return (arr, m) -> {
                arr[0] = m.value < arr[0] || arr[3] == 1 ? m.value : arr[0];
                arr[1] = m.value > arr[1] || arr[3] == 1 ? m.value : arr[1];

                arr[2] += m.value;
                arr[3]++;
            };
        }

        @Override
        public BinaryOperator<double[]> combiner() {
            return (arr1, arr2) -> {
                var combination = new double[4];
                combination[0] = arr1[0] < arr2[0] ? arr1[0] : arr2[0];
                combination[1] = arr1[1] > arr2[1] ? arr1[1] : arr2[1];

                combination[2] = arr1[2] + arr2[2];
                combination[3] = arr1[3] + arr2[3];

                return combination;
            };
        }

        @Override
        public Function<double[], String> finisher() {
            return arr -> {
                var mean = (arr[3] == 0) ? 0.0d : Math.round((arr[2] / arr[3]) * 10.0) / 10.0;
                var max = arr[0];
                var min = arr[1];
                return min + "/" + mean + "/" + max;
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }

    private static final AverageCollector AVERAGE_COLLECTOR = new AverageCollector();

    private static class FileChannelIterator implements Iterator<ByteBuffer> {

        private static final long CHUNK_SIZE = (long) Math.pow(2, 20);

        private final FileChannel fileChannel;

        private final long size;
        private long bytesRead = 0;

        private FileChannelIterator(FileChannel fileChannel) {
            this.fileChannel = fileChannel;
            try {
                this.size = fileChannel.size();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.bytesRead < size;
        }

        @Override
        public ByteBuffer next() {
            try {
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, bytesRead, Math.min(CHUNK_SIZE, size - bytesRead));

                // Ensure the chunks will end on a newline
                int realEnd = mappedByteBuffer.limit() - 1;
                while (mappedByteBuffer.get(realEnd) != '\n') {
                    realEnd--;
                }
                mappedByteBuffer.limit(++realEnd);
                bytesRead += realEnd;

                return mappedByteBuffer;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private record Measurement(String station, double value) {
    }

    public static void main(String[] args) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(FILE, "r");
        FileChannel fileChannel = randomAccessFile.getChannel();

        Stream<ByteBuffer> byteBufferStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(new FileChannelIterator(fileChannel), Spliterator.IMMUTABLE),
                true);

        Stream<Measurement> measurements = byteBufferStream.flatMap(a -> parseMeasurements(a).stream());

        TreeMap<String, String> processedStations = measurements.collect(groupingBy(Measurement::station, TreeMap::new, AVERAGE_COLLECTOR));

        System.out.println(processedStations);
    }

    // The following code is just copied from @twobiers solution, since I took it as an example to analyze.
    // The code above was at least studied.

    private static List<Measurement> parseMeasurements(ByteBuffer byteBuffer) {
        // Most of the code here is derived from @bjhara's implementation
        // https://github.com/gunnarmorling/1brc/pull/10
        var measurements = new ArrayList<Measurement>(100_000);

        final int limit = byteBuffer.limit();
        final byte[] buffer = new byte[128];

        while (byteBuffer.position() < limit) {
            final int start = byteBuffer.position();

            int separatorPosition = start;
            while (separatorPosition != limit && byteBuffer.get(separatorPosition) != ';') {
                separatorPosition++;
            }

            int endOfLinePosition = separatorPosition; // must be after the separator
            while (endOfLinePosition != limit && byteBuffer.get(endOfLinePosition) != '\n') {
                endOfLinePosition++;
            }

            int nameOffset = separatorPosition - start;
            byteBuffer.get(buffer, 0, nameOffset);
            String key = new String(buffer, 0, nameOffset);

            byteBuffer.get(); // Skip separator

            int valueLength = endOfLinePosition - separatorPosition - 1;
            byteBuffer.get(buffer, 0, valueLength);
            double value = fastParseDouble(buffer, valueLength);

            byteBuffer.get(); // Skip newline

            measurements.add(new Measurement(key, value));
        }

        return measurements;
    }

    private static double fastParseDouble(byte[] bytes, int length) {
        long value = 0;
        int exp = 0;
        boolean negative = false;
        int decimalPlaces = Integer.MIN_VALUE;
        for (int i = 0; i < length; i++) {
            byte ch = bytes[i];
            if (ch >= '0' && ch <= '9') {
                value = value * 10 + (ch - '0');
                decimalPlaces++;
            }
            else if (ch == '-') {
                negative = true;
            }
            else if (ch == '.') {
                decimalPlaces = 0;
            }
        }

        return asDouble(value, exp, negative, decimalPlaces);
    }

    private static double asDouble(long value, int exp, boolean negative, int decimalPlaces) {
        if (decimalPlaces > 0 && value < Long.MAX_VALUE / 2) {
            if (value < Long.MAX_VALUE / (1L << 32)) {
                exp -= 32;
                value <<= 32;
            }
            if (value < Long.MAX_VALUE / (1L << 16)) {
                exp -= 16;
                value <<= 16;
            }
            if (value < Long.MAX_VALUE / (1L << 8)) {
                exp -= 8;
                value <<= 8;
            }
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
            }
        }
        for (; decimalPlaces > 0; decimalPlaces--) {
            exp--;
            long mod = value % 5;
            value /= 5;
            int modDiv = 1;
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
                modDiv <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
                modDiv <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
                modDiv <<= 1;
            }
            if (decimalPlaces > 1) {
                value += modDiv * mod / 5;
            }
            else {
                value += (modDiv * mod + 4) / 5;
            }
        }
        final double d = Math.scalb((double) value, exp);
        return negative ? -d : d;
    }

}

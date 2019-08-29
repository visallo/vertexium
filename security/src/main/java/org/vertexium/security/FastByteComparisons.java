/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.vertexium.security;

/**
 * Utility code to do optimized byte-array comparison.
 * This is borrowed and slightly modified from Guava's UnsignedBytes
 * class to be able to compare arrays that start at non-zero offsets.
 */
abstract class FastByteComparisons {
    /**
     * Lexicographically compare two byte arrays.
     */
    public static int compareTo(byte[] b1, int s1, int l1, byte[] b2, int s2,
                                int l2) {
        return LexicographicalComparerHolder.BEST_COMPARER.compareTo(
            b1, s1, l1, b2, s2, l2);
    }


    private interface Comparer<T> {
        int compareTo(T buffer1, int offset1, int length1,
                      T buffer2, int offset2, int length2);
    }

    private static Comparer<byte[]> lexicographicalComparerJavaImpl() {
        return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
    }


    /**
     * Provides a lexicographical comparer implementation; either a Java
     * implementation or a faster implementation based on Unsafe.
     *
     * <p>Uses reflection to gracefully fall back to the Java implementation if
     * {@code Unsafe} isn't available.
     */
    private static class LexicographicalComparerHolder {
        static final Comparer<byte[]> BEST_COMPARER = getBestComparer();

        /**
         * Returns the Unsafe-using Comparer, or falls back to the pure-Java
         * implementation if unable to do so.
         */
        static Comparer<byte[]> getBestComparer() {
            return lexicographicalComparerJavaImpl();
        }

        private enum PureJavaComparer implements Comparer<byte[]> {
            INSTANCE;

            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1,
                                 byte[] buffer2, int offset2, int length2) {
                // Short circuit equal case
                if (buffer1 == buffer2 &&
                    offset1 == offset2 &&
                    length1 == length2) {
                    return 0;
                }
                // Bring WritableComparator code local
                int end1 = offset1 + length1;
                int end2 = offset2 + length2;
                for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                    int a = (buffer1[i] & 0xff);
                    int b = (buffer2[j] & 0xff);
                    if (a != b) {
                        return a - b;
                    }
                }
                return length1 - length2;
            }
        }
    }
}

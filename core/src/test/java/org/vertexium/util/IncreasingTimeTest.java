package org.vertexium.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IncreasingTimeTest {
    private static final int NUM_ITERATIONS = 1000;
    private static final int NUM_THREADS = 10;

    @Test
    public void currentTimeMillisReturnsEverIncreasingTime() {
        List<Long> times = new ArrayList<>(NUM_ITERATIONS);
        // iterate without sleeping to insure IncreasingTime will encounter duplicate system times.
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            times.add(IncreasingTime.currentTimeMillis());
        }
        assertIncreasingTimes(NUM_ITERATIONS, times);
    }

    @Test
    public void currentTimeMillisReturnsTimeGreaterThanSystemTime() {
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            long systemTime = System.currentTimeMillis();
            long increasedTime = IncreasingTime.currentTimeMillis();
            assertTrue(increasedTime > systemTime);
        }
    }

    @Test
    public void currentTimeMillisReturnsEverIncreasingTimeAcrossConcurrentThreads() throws Exception {
        final List<Long> times = Collections.synchronizedList(new ArrayList<Long>(NUM_ITERATIONS));
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    for (int i = 0; i < NUM_ITERATIONS; i++) {
                        times.add(IncreasingTime.currentTimeMillis());
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join(5000);
        }
        Collections.sort(times);
        assertIncreasingTimes(NUM_THREADS * NUM_ITERATIONS, times);
    }

    private static void assertIncreasingTimes(int expectedSize, List<Long> times) {
        assertEquals(expectedSize, new HashSet<>(times).size()); // the set removes duplicates
        long last = times.get(0) - 1;
        for (long time : times) {
            assertTrue(time == last + 1);
            last = time;
        }
    }
}

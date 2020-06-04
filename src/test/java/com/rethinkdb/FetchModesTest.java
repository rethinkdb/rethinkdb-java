package com.rethinkdb;

import com.rethinkdb.net.Result.FetchMode;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FetchModesTest {
    /**
     * Least common multiple of 2, 3, 4, 5, 6, 7, 8.
     * Size is not realistic, but makes the divisions easier.
     *
     */
    private static final int REQUEST_SIZE = 840;

    @Test
    public void testBasic() {
        FetchMode[] values = FetchMode.values();
        for (FetchMode value : values) {
            assertTrue(value.shouldContinue(0, REQUEST_SIZE));
        }
    }

    @Test
    public void testAggressive() {
        for (int i = 0; i < REQUEST_SIZE; i++) {
            assertTrue(FetchMode.AGGRESSIVE.shouldContinue(i, REQUEST_SIZE));
        }
    }

    @Test
    public void testLazy() {
        for (int i = 1; i < REQUEST_SIZE; i++) {
            assertFalse(FetchMode.LAZY.shouldContinue(i, REQUEST_SIZE));
        }
        assertTrue(FetchMode.LAZY.shouldContinue(0, REQUEST_SIZE));
    }

    @Test
    public void testPreemptive() {
        testPreemptiveImpl(2, FetchMode.PREEMPTIVE_HALF);
        testPreemptiveImpl(3, FetchMode.PREEMPTIVE_THIRD);
        testPreemptiveImpl(4, FetchMode.PREEMPTIVE_FOURTH);
        testPreemptiveImpl(5, FetchMode.PREEMPTIVE_FIFTH);
        testPreemptiveImpl(6, FetchMode.PREEMPTIVE_SIXTH);
        testPreemptiveImpl(7, FetchMode.PREEMPTIVE_SEVENTH);
        testPreemptiveImpl(8, FetchMode.PREEMPTIVE_EIGHTH);
    }

    private void testPreemptiveImpl(int splitAt, FetchMode mode) {
        for (int i = 0; i <= REQUEST_SIZE / splitAt; i++) {
            assertTrue(mode.shouldContinue(i, REQUEST_SIZE));
        }
        for (int i = REQUEST_SIZE / splitAt + 1; i < REQUEST_SIZE; i++) {
            assertFalse(mode.shouldContinue(i, REQUEST_SIZE));
        }
    }
}

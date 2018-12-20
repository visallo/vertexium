package org.vertexium.id;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleNameSubstitutionStrategyTest {
    private static List<Pair<String, String>> templatePashingTest;
    private SimpleNameSubstitutionStrategy testSubject;

    private static final String KEY1 = "test";
    private static final String KEY2 = "tint";
    private static final String VALUE1 = "t";
    private static final String VALUE2 = "p";

    @BeforeAll
    public static void beforeClass() {
        templatePashingTest = Lists.newArrayList();
        templatePashingTest.add(Pair.of(KEY1, VALUE1));
        templatePashingTest.add(Pair.of(KEY2, VALUE2));
    }

    @BeforeEach
    public void before() {
        testSubject = new SimpleNameSubstitutionStrategy();
        testSubject.setSubstitutionList(templatePashingTest);
    }

    @Test
    public void testDeflate() {
        String test = testSubject.deflate(KEY1);
        assertEquals(SimpleNameSubstitutionStrategy.wrap(VALUE1), test);
    }

    @Test
    public void testDeflateMultiple() {
        String test = testSubject.deflate(KEY1 + KEY1);
        assertEquals(SimpleNameSubstitutionStrategy.wrap(VALUE1) + SimpleNameSubstitutionStrategy.wrap(VALUE1), test);
    }

    @Test
    public void testDeflateCache() {
        assertEquals(0, testSubject.getDeflateCacheMisses());
        assertEquals(0, testSubject.getDeflateCalls());

        testSubject.deflate(KEY1);
        assertEquals(1, testSubject.getDeflateCacheMisses());
        assertEquals(1, testSubject.getDeflateCalls());

        testSubject.deflate(KEY1);
        assertEquals(1, testSubject.getDeflateCacheMisses());
        assertEquals(2, testSubject.getDeflateCalls());
    }

    @Test
    public void testInflate() {
        String test = testSubject.inflate(SimpleNameSubstitutionStrategy.wrap(VALUE1));
        assertEquals(KEY1, test);
    }

    @Test
    public void testInflateMultiple() {
        String test = testSubject.inflate(SimpleNameSubstitutionStrategy.wrap(VALUE1) + SimpleNameSubstitutionStrategy.wrap(VALUE1));
        assertEquals(KEY1 + KEY1, test);
    }

    @Test
    public void testInflateCache() {
        assertEquals(0, testSubject.getInflateCacheMisses());
        assertEquals(0, testSubject.getInflateCalls());

        testSubject.inflate(SimpleNameSubstitutionStrategy.wrap(VALUE1));
        assertEquals(1, testSubject.getInflateCacheMisses());
        assertEquals(1, testSubject.getInflateCalls());

        testSubject.inflate(SimpleNameSubstitutionStrategy.wrap(VALUE1));
        assertEquals(1, testSubject.getInflateCacheMisses());
        assertEquals(2, testSubject.getInflateCalls());
    }

    @Test
    public void testSubstitutionIsInvertible() {
        String test = testSubject.inflate(testSubject.deflate(KEY1));
        assertEquals(KEY1, test);
    }

    @Test
    public void testNonSubstitutionReturnsOriginalInput() {
        String test = testSubject.deflate("misspelled tets");
        assertEquals("misspelled tets", test);
    }

    @Test
    public void testNoSubstitutionsIsInvertable() {
        String test = testSubject.inflate(testSubject.deflate("misspelled tets"));
        assertEquals("misspelled tets", test);
    }

    @Test
    public void testMultipleSubstitutionsGetSubstitutedMultipleTimes() {
        String test = testSubject.deflate(KEY1 + KEY1);
        assertEquals(SimpleNameSubstitutionStrategy.wrap(VALUE1) + SimpleNameSubstitutionStrategy.wrap(VALUE1), test);
    }

    @Test
    public void testMultipleSameSubstitutionsAreCorrectlyReturned() {
        String multipleSubstitutionString = KEY1 + KEY1;
        String multipleSubstitutions = testSubject.inflate(testSubject.deflate(multipleSubstitutionString));
        assertEquals(multipleSubstitutionString, multipleSubstitutions);
    }

    @Test
    public void testMultipleDifferentSubstitutionsAreCorrectlyReturned() {
        String multipleSubstitutionString = KEY1 + KEY2;
        String multipleSubstitutions = testSubject.inflate(testSubject.deflate(multipleSubstitutionString));
        assertEquals(multipleSubstitutionString, multipleSubstitutions);
    }

    @Test
    public void testMultipleSubstitutionsWorkInOrder() {
        String test = testSubject.deflate("testint");
        assertEquals(SimpleNameSubstitutionStrategy.wrap(VALUE1) + "int", test);
    }

    @Test
    public void testMultipleSubstitutionsWorkInOrder1() {
        String test = testSubject.deflate("tintest");
        assertEquals("tin" + SimpleNameSubstitutionStrategy.wrap(VALUE1), test);
    }
}

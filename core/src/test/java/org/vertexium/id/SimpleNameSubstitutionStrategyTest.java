package org.vertexium.id;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class SimpleNameSubstitutionStrategyTest {
    private static List<Pair<String, String>> templatePashingTest;
    private SimpleNameSubstitutionStrategy testSubject;

    private static final String KEY1 = "test";
    private static final String KEY2 = "tint";
    private static final String VALUE1 = "t";
    private static final String VALUE2 = "p";

    @BeforeClass
    public static void beforeClass() {
        templatePashingTest = Lists.newArrayList();
        templatePashingTest.add(Pair.of(KEY1, VALUE1));
        templatePashingTest.add(Pair.of(KEY2, VALUE2));
    }

    @Before
    public void before() {
        testSubject = new SimpleNameSubstitutionStrategy();
        testSubject.setSubstitutionList(templatePashingTest);
    }

    @Test
    public void testDeflate() {
        String test = testSubject.deflate(KEY1);
        assertThat(test, is(SimpleNameSubstitutionStrategy.wrap(VALUE1)));
    }

    @Test
    public void testDeflateMultiple() {
        String test = testSubject.deflate(KEY1 + KEY1);
        assertThat(test, is(SimpleNameSubstitutionStrategy.wrap(VALUE1) + SimpleNameSubstitutionStrategy.wrap(VALUE1)));
    }

    @Test
    public void testInflate() {
        String test = testSubject.inflate(SimpleNameSubstitutionStrategy.wrap(VALUE1));
        assertThat(test, is(KEY1));
    }

    @Test
    public void testInflateMultiple() {
        String test = testSubject.inflate(SimpleNameSubstitutionStrategy.wrap(VALUE1) + SimpleNameSubstitutionStrategy.wrap(VALUE1));
        assertThat(test, is(KEY1 + KEY1));
    }

    @Test
    public void testSubstitutionIsInvertible() {
        String test = testSubject.inflate(testSubject.deflate(KEY1));
        assertThat(test, is(KEY1));
    }

    @Test
    public void testNonSubstitutionReturnsOriginalInput() {
        String test = testSubject.deflate("misspelled tets");
        assertThat(test, is("misspelled tets"));
    }

    @Test
    public void testNoSubstitutionsIsInvertable() {
        String test = testSubject.inflate(testSubject.deflate("misspelled tets"));
        assertThat(test, is("misspelled tets"));
    }

    @Test
    public void testMultipleSubstitutionsGetSubstitutedMultipleTimes() {
        String test = testSubject.deflate(KEY1 + KEY1);
        assertThat(test, is(SimpleNameSubstitutionStrategy.wrap(VALUE1) + SimpleNameSubstitutionStrategy.wrap(VALUE1)));
    }

    @Test
    public void testMultipleSameSubstitutionsAreCorrectlyReturned() {
        String multipleSubstitutionString = KEY1 + KEY1;
        String multipleSubstitutions = testSubject.inflate(testSubject.deflate(multipleSubstitutionString));
        assertThat(multipleSubstitutions, is(multipleSubstitutionString));
    }

    @Test
    public void testMultipleDifferentSubstitutionsAreCorrectlyReturned() {
        String multipleSubstitutionString = KEY1 + KEY2;
        String multipleSubstitutions = testSubject.inflate(testSubject.deflate(multipleSubstitutionString));
        assertThat(multipleSubstitutions, is(multipleSubstitutionString));
    }

    @Test
    public void testMultipleSubstitutionsWorkInOrder() {
        String test = testSubject.deflate("testint");
        assertThat(test, is(SimpleNameSubstitutionStrategy.wrap(VALUE1) + "int"));
    }

    @Test
    public void testMultipleSubstitutionsWorkInOrder1() {
        String test = testSubject.deflate("tintest");
        assertThat(test, is("tin" + SimpleNameSubstitutionStrategy.wrap(VALUE1)));
    }
}

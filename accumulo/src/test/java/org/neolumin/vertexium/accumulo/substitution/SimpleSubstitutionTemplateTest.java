package org.neolumin.vertexium.accumulo.substitution;

import com.beust.jcommander.internal.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neolumin.vertexium.accumulo.substitution.SimpleSubstitutionTemplate;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class SimpleSubstitutionTemplateTest {
    private static List<Pair<String, String>> templatePashingTest;
    private SimpleSubstitutionTemplate testSubject;

    private static final String KEY1 = "test";
    private static final String KEY2 = "tint";
    private static final String VALUE1 = "t";
    private static final String VALUE2 = "p";

    @BeforeClass
    public static void beforeClass(){
        templatePashingTest = Lists.newArrayList();
        templatePashingTest.add(Pair.of(KEY1, VALUE1));
        templatePashingTest.add(Pair.of(KEY2, VALUE2));
    }

    @Before
    public void before(){
        testSubject = new SimpleSubstitutionTemplate(templatePashingTest);
    }

    @Test
    public void testSubstitutionIsSwappedOut(){
        String test = testSubject.deflate(KEY1);
        assertThat(test, is(SimpleSubstitutionTemplate.wrap(VALUE1)));
    }

    @Test
    public void testSubstitionIsInvertible(){
        String test = testSubject.inflate(testSubject.deflate(KEY1));
        assertThat(test, is(KEY1));
    }

    @Test
    public void testNonSubstitutionReturnsOriginalInput(){
        String test = testSubject.deflate("misspelled tets");
        assertThat(test, is("misspelled tets"));
    }

    @Test
    public void testNoSubstitionsIsInvertable(){
        String test = testSubject.inflate(testSubject.deflate("misspelled tets"));
        assertThat(test, is("misspelled tets"));
    }

    @Test
    public void testMultipleSubstitutionsGetSubstitutedMultipleTimes(){
        String test = testSubject.deflate(KEY1 + KEY1);
        assertThat(test, is(SimpleSubstitutionTemplate.wrap(VALUE1) + SimpleSubstitutionTemplate.wrap(VALUE1)));
    }

    @Test
    public void testMultipleSameSubstituionsAreCorrectlyReturned(){
        String multipleSubstitutionString = KEY1 + KEY1;
        String multipleSubstituions = testSubject.inflate(testSubject.deflate(multipleSubstitutionString));
        assertThat(multipleSubstituions, is(multipleSubstitutionString));
    }
    @Test
    public void testMultipleDifferentSubstituionsAreCorrectlyReturned(){
        String multipleSubstitutionString = KEY1 + KEY2;
        String multipleSubstituions = testSubject.inflate(testSubject.deflate(multipleSubstitutionString));
        assertThat(multipleSubstituions, is(multipleSubstitutionString));
    }

    @Test
    public void testMultipleSubstituionsWorkInOrder(){
        String test = testSubject.deflate("testint");
        assertThat(test, is(SimpleSubstitutionTemplate.wrap(VALUE1) + "int"));
    }

    @Test
    public void testMultipleSubstituionsWorkInOrder1(){
        String test = testSubject.deflate("tintest");
        assertThat(test, is("tin" + SimpleSubstitutionTemplate.wrap(VALUE1)));
    }
}

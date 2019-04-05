package org.vertexium.accumulo.iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.junit.runners.Parameterized.Parameters;

public class DeduplicationIteratorHarnessTest extends BaseJUnit4IteratorTest{
    @Parameters
    public static Object[][] parameters() {
        IteratorTestInput input = getIteratorInput();
        IteratorTestOutput output = getIteratorOutput();
        List<IteratorTestCase> tests = IteratorTestCaseFinder.findAllTestCases();
        return BaseJUnit4IteratorTest.createParameters(input, output, tests);
    }

    private static final TreeMap<Key,Value> INPUT_DATA = createInputData();
    private static final TreeMap<Key,Value> OUTPUT_DATA = createOutputData();

    private static TreeMap<Key,Value> createInputData() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 2), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 3), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 4), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 5), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 6), new Value("red"));
        return tm;
    }

    private static TreeMap<Key,Value> createOutputData() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key,Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 5), new Value("red"));
        return tm;
    }

    private static IteratorTestInput getIteratorInput() {
        return new IteratorTestInput(DeduplicationIterator.class, Collections.emptyMap(), new Range(),
                INPUT_DATA);
    }

    private static IteratorTestOutput getIteratorOutput() {
        return new IteratorTestOutput(OUTPUT_DATA);
    }

    public DeduplicationIteratorHarnessTest(IteratorTestInput input, IteratorTestOutput expectedOutput,
                                IteratorTestCase testCase) {
        super(input, expectedOutput, testCase);
    }

}

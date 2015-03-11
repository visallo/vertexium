package org.neolumin.vertexium.accumulo;

import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.neolumin.vertexium.accumulo.AccumuloGraphConfiguration.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(JUnit4.class)
public class AccumuloGraphConfigurationTest {

    @Test
    public void testBatchWriterConfigUsesDefaultWithNoParameters(){
        Map configMap = Maps.newHashMap();
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(configMap);
        BatchWriterConfig batchWriterConfig = accumuloGraphConfiguration.createBatchWriterConfig();

        assertThat(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), is(DEFAULT_BATCHWRITER_MAX_LATENCY));
        assertThat(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), is(DEFAULT_BATCHWRITER_TIMEOUT));
        assertThat(batchWriterConfig.getMaxMemory(), is(DEFAULT_BATCHWRITER_MAX_MEMORY));
        assertThat(batchWriterConfig.getMaxWriteThreads(), is(DEFAULT_BATCHWRITER_MAX_WRITE_THREADS));
    }

    @Test
    public void testBatchWriterConfigIsSetToValuesWithParameters(){
        int numThreads = 2;
        long timeout = 3;
        long maxMemory = 5;
        long maxLatency = 7;

        Map configMap = Maps.newHashMap();
        MapUtils.putAll(configMap, new String[] {
                BATCHWRITER_MAX_LATENCY, "" + maxLatency,
                BATCHWRITER_MAX_MEMORY, "" + maxMemory,
                BATCHWRITER_MAX_WRITE_THREADS, "" + numThreads,
                BATCHWRITER_TIMEOUT, "" + timeout });
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(configMap);
        BatchWriterConfig batchWriterConfig = accumuloGraphConfiguration.createBatchWriterConfig();

        assertThat(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), is(maxLatency));
        assertThat(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), is(timeout));
        assertThat(batchWriterConfig.getMaxMemory(), is(maxMemory));
        assertThat(batchWriterConfig.getMaxWriteThreads(), is(numThreads));
    }
}

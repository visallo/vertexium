package org.vertexium.accumulo;

import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(JUnit4.class)
public class AccumuloGraphConfigurationTest {

    @Test
    public void testBatchWriterConfigUsesDefaultWithNoParameters() {
        Map configMap = Maps.newHashMap();
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(configMap);
        BatchWriterConfig batchWriterConfig = accumuloGraphConfiguration.createBatchWriterConfig();

        assertThat(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_LATENCY));
        assertThat(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_TIMEOUT));
        assertThat(batchWriterConfig.getMaxMemory(), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_MEMORY));
        assertThat(batchWriterConfig.getMaxWriteThreads(), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_WRITE_THREADS));
    }

    @Test
    public void testBatchWriterConfigIsSetToValuesWithParameters() {
        int numThreads = 2;
        long timeout = 3;
        long maxMemory = 5;
        long maxLatency = 7;

        Map configMap = Maps.newHashMap();
        MapUtils.putAll(configMap, new String[]{
                AccumuloGraphConfiguration.BATCHWRITER_MAX_LATENCY, "" + maxLatency,
                AccumuloGraphConfiguration.BATCHWRITER_MAX_MEMORY, "" + maxMemory,
                AccumuloGraphConfiguration.BATCHWRITER_MAX_WRITE_THREADS, "" + numThreads,
                AccumuloGraphConfiguration.BATCHWRITER_TIMEOUT, "" + timeout});
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(configMap);
        BatchWriterConfig batchWriterConfig = accumuloGraphConfiguration.createBatchWriterConfig();

        assertThat(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), is(maxLatency));
        assertThat(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), is(timeout));
        assertThat(batchWriterConfig.getMaxMemory(), is(maxMemory));
        assertThat(batchWriterConfig.getMaxWriteThreads(), is(numThreads));
    }
}

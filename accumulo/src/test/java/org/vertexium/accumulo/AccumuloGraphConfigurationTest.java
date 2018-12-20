package org.vertexium.accumulo;

import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AccumuloGraphConfigurationTest {

    @Test
    public void testBatchWriterConfigUsesDefaultWithNoParameters() {
        Map configMap = Maps.newHashMap();
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(configMap);
        BatchWriterConfig batchWriterConfig = accumuloGraphConfiguration.createBatchWriterConfig();

        assertEquals(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), (long) AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_LATENCY);
        assertEquals(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), (long) AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_TIMEOUT);
        assertEquals(batchWriterConfig.getMaxMemory(), (long) AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_MEMORY);
        assertEquals(batchWriterConfig.getMaxWriteThreads(), (long) AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_WRITE_THREADS);
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

        assertEquals(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), maxLatency);
        assertEquals(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), timeout);
        assertEquals(batchWriterConfig.getMaxMemory(), maxMemory);
        assertEquals(batchWriterConfig.getMaxWriteThreads(), numThreads);
    }
}

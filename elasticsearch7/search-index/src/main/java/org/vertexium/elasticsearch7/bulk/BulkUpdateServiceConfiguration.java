package org.vertexium.elasticsearch7.bulk;


import java.time.Duration;

public class BulkUpdateServiceConfiguration {
    public static final int MAX_POOL_SIZE_DEFAULT = Runtime.getRuntime().availableProcessors() * 2;
    public static final int CORE_POOL_SIZE_DEFAULT = Math.min(2, MAX_POOL_SIZE_DEFAULT / 2);
    public static final Duration BULK_REQUEST_TIMEOUT_DEFAULT = Duration.ofMinutes(30);
    public static final int MAX_BATCH_SIZE_DEFAULT = 100;
    public static final int MAX_BATCH_SIZE_IN_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final Duration BATCH_WINDOW_TIME_DEFAULT = Duration.ofMillis(100);
    public static final int MAX_FAIL_COUNT_DEFAULT = 10;
    private int corePoolSize = CORE_POOL_SIZE_DEFAULT;
    private int maximumPoolSize = MAX_POOL_SIZE_DEFAULT;
    private Duration bulkRequestTimeout = BULK_REQUEST_TIMEOUT_DEFAULT;
    private int maxBatchSize = MAX_BATCH_SIZE_DEFAULT;
    private int maxBatchSizeInBytes = MAX_BATCH_SIZE_IN_BYTES_DEFAULT;
    private Duration batchWindowTime = BATCH_WINDOW_TIME_DEFAULT;
    private int maxFailCount = MAX_FAIL_COUNT_DEFAULT;

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public BulkUpdateServiceConfiguration setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
        return this;
    }

    public Duration getBulkRequestTimeout() {
        return bulkRequestTimeout;
    }

    public BulkUpdateServiceConfiguration setBulkRequestTimeout(Duration bulkRequestTimeout) {
        this.bulkRequestTimeout = bulkRequestTimeout;
        return this;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public BulkUpdateServiceConfiguration setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
        return this;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public BulkUpdateServiceConfiguration setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public BulkUpdateServiceConfiguration setMaxBatchSizeInBytes(int maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        return this;
    }

    public int getMaxFailCount() {
        return maxFailCount;
    }

    public BulkUpdateServiceConfiguration setMaxFailCount(int maxFailCount) {
        this.maxFailCount = maxFailCount;
        return this;
    }

    public Duration getBatchWindowTime() {
        return batchWindowTime;
    }

    public BulkUpdateServiceConfiguration setBatchWindowTime(Duration batchWindowTime) {
        this.batchWindowTime = batchWindowTime;
        return this;
    }
}

package org.vertexium.elasticsearch5.bulk;


import java.time.Duration;

public class BulkUpdateServiceConfiguration {
    public static final int QUEUE_DEPTH_DEFAULT = 20;
    public static final int CORE_POOL_SIZE_DEFAULT = 10;
    public static final int MAX_POOL_SIZE_DEFAULT = 10;
    public static final Duration BULK_REQUEST_TIMEOUT_DEFAULT = Duration.ofMinutes(30);
    public static final int MAX_BATCH_SIZE_DEFAULT = BulkUpdateQueueConfiguration.MAX_BATCH_SIZE_DEFAULT;
    public static final int MAX_BATCH_SIZE_IN_BYTES_DEFAULT = BulkUpdateQueueConfiguration.MAX_BATCH_SIZE_IN_BYTES_DEFAULT;
    public static final int MAX_FAIL_COUNT_DEFAULT = BulkUpdateQueueConfiguration.MAX_FAIL_COUNT_DEFAULT;
    private int queueDepth = QUEUE_DEPTH_DEFAULT;
    private int corePoolSize = CORE_POOL_SIZE_DEFAULT;
    private int maximumPoolSize = MAX_POOL_SIZE_DEFAULT;
    private Duration bulkRequestTimeout = BULK_REQUEST_TIMEOUT_DEFAULT;
    private int maxBatchSize = MAX_BATCH_SIZE_DEFAULT;
    private int maxBatchSizeInBytes = MAX_BATCH_SIZE_IN_BYTES_DEFAULT;
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

    public int getQueueDepth() {
        return queueDepth;
    }

    public BulkUpdateServiceConfiguration setQueueDepth(int queueDepth) {
        this.queueDepth = queueDepth;
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
}

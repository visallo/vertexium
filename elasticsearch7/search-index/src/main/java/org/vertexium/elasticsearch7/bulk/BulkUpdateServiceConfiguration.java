package org.vertexium.elasticsearch7.bulk;


import java.time.Duration;

public class BulkUpdateServiceConfiguration {
    public static final int BACKLOG_SIZE_DEFAULT = 100;
    public static final Duration BATCH_WINDOW_TIME_DEFAULT = Duration.ofMillis(1000);
    public static final Duration BULK_REQUEST_TIMEOUT_DEFAULT = Duration.ofMinutes(30);
    public static final Integer LOG_REQUEST_SIZE_LIMIT_DEFAULT = null;
    public static final int MAX_BATCH_SIZE_DEFAULT = 1000;
    public static final int MAX_BATCH_SIZE_IN_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final int MAX_FAIL_COUNT_DEFAULT = 10;
    public static final int POOL_SIZE_DEFAULT = 10;
    private int backlogSize = BACKLOG_SIZE_DEFAULT;
    private Duration batchWindowTime = BATCH_WINDOW_TIME_DEFAULT;
    private Duration bulkRequestTimeout = BULK_REQUEST_TIMEOUT_DEFAULT;
    private Integer logRequestSizeLimit = LOG_REQUEST_SIZE_LIMIT_DEFAULT;
    private int maxBatchSize = MAX_BATCH_SIZE_DEFAULT;
    private int maxBatchSizeInBytes = MAX_BATCH_SIZE_IN_BYTES_DEFAULT;
    private int maxFailCount = MAX_FAIL_COUNT_DEFAULT;
    private int poolSize = POOL_SIZE_DEFAULT;

    public int getBacklogSize() {
        return backlogSize;
    }

    public BulkUpdateServiceConfiguration setBacklogSize(int backlogSize) {
        this.backlogSize = backlogSize;
        return this;
    }

    public Duration getBatchWindowTime() {
        return batchWindowTime;
    }

    public BulkUpdateServiceConfiguration setBatchWindowTime(Duration batchWindowTime) {
        this.batchWindowTime = batchWindowTime;
        return this;
    }

    public Duration getBulkRequestTimeout() {
        return bulkRequestTimeout;
    }

    public BulkUpdateServiceConfiguration setBulkRequestTimeout(Duration bulkRequestTimeout) {
        this.bulkRequestTimeout = bulkRequestTimeout;
        return this;
    }

    public Integer getLogRequestSizeLimit() {
        return logRequestSizeLimit;
    }

    public BulkUpdateServiceConfiguration setLogRequestSizeLimit(Integer logRequestSizeLimit) {
        this.logRequestSizeLimit = logRequestSizeLimit;
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

    public int getPoolSize() {
        return poolSize;
    }

    public BulkUpdateServiceConfiguration setPoolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }
}

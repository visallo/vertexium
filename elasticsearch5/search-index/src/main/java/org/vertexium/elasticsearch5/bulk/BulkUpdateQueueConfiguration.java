package org.vertexium.elasticsearch5.bulk;

public class BulkUpdateQueueConfiguration {
    public static final int MAX_BATCH_SIZE_DEFAULT = 100;
    public static final int MAX_BATCH_SIZE_IN_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final int MAX_FAIL_COUNT_DEFAULT = 10;
    private int maxBatchSize = MAX_BATCH_SIZE_DEFAULT;
    private int maxBatchSizeInBytes = MAX_BATCH_SIZE_IN_BYTES_DEFAULT;
    private int maxFailCount = MAX_FAIL_COUNT_DEFAULT;

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public BulkUpdateQueueConfiguration setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public BulkUpdateQueueConfiguration setMaxBatchSizeInBytes(int maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        return this;
    }

    public int getMaxFailCount() {
        return maxFailCount;
    }

    public BulkUpdateQueueConfiguration setMaxFailCount(int maxFailCount) {
        this.maxFailCount = maxFailCount;
        return this;
    }
}

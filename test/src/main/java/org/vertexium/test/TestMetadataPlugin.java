package org.vertexium.test;

import com.google.common.collect.Lists;
import org.vertexium.*;

import java.util.ArrayList;
import java.util.List;

public class TestMetadataPlugin extends DefaultMetadataPlugin {
    private static boolean enabled;
    private static int skippedMetadataEntriesCount;
    private List<Metadata.Entry> defaultEntries = Lists.newArrayList(
        new Metadata.Entry("metadataKey1", "defaultValue", new Visibility(""))
    );

    public static void clear() {
        enabled = false;
        skippedMetadataEntriesCount = 0;
    }

    public static void enable(boolean enabled) {
        TestMetadataPlugin.enabled = enabled;
    }

    public static int getSkippedMetadataEntriesCount() {
        return skippedMetadataEntriesCount;
    }

    @Override
    public boolean shouldWriteMetadata(
        ElementId elementId,
        Property property,
        String metadataKey,
        Visibility metadataVisibility,
        Object metadataValue,
        long propertyTimestamp
    ) {
        if (!enabled) {
            return true;
        }
        if ("metadataKey1".equals(metadataKey) && "defaultValue".equals(metadataValue)) {
            skippedMetadataEntriesCount++;
            return false;
        }
        if ("modifiedDate".equals(metadataKey) && metadataValue instanceof Long && (Long) metadataValue == propertyTimestamp) {
            skippedMetadataEntriesCount++;
            return false;
        }
        return super.shouldWriteMetadata(elementId, property, metadataKey, metadataVisibility, metadataValue, propertyTimestamp);
    }

    @Override
    public List<Metadata.Entry> getAllDefaultEntries(long propertyTimestamp, FetchHints fetchHints) {
        if (!enabled) {
            return super.getAllDefaultEntries(propertyTimestamp, fetchHints);
        }
        List<Metadata.Entry> entries = new ArrayList<>(defaultEntries);
        entries.add(new Metadata.Entry("modifiedDate", propertyTimestamp, new Visibility("")));
        return filterEntriesByFetchHints(entries, fetchHints);
    }
}

package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Set;

public class Property {
    public final String key;
    public final String name;
    public final byte[] value;
    public final Set<Text> hiddenVisibilities;
    public final String visibility;
    public final long timestamp;
    public final List<Integer> metadata;

    public Property(
            String propertyKey,
            String propertyName,
            byte[] propertyValue,
            String propertyVisibility,
            long propertyTimestamp,
            Set<Text> propertyHiddenVisibilities,
            List<Integer> metadata
    ) {
        this.key = propertyKey;
        this.name = propertyName;
        this.value = propertyValue;
        this.visibility = propertyVisibility;
        this.timestamp = propertyTimestamp;
        this.hiddenVisibilities = propertyHiddenVisibilities;
        this.metadata = metadata;
    }
}

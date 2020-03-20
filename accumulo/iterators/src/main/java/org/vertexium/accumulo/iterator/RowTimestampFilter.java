package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.util.OptionsUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RowTimestampFilter extends Filter {
    public static final String TIMESTAMPS = "timestamps";
    private Map<Text, Timestamp> timestamps;

    public static void setTimestamps(IteratorSetting iteratorSetting, Map<Text, Timestamp> timestamps) {
        StringBuilder value = new StringBuilder();
        boolean first = true;
        for (Map.Entry<Text, Timestamp> entry : timestamps.entrySet()) {
            if (!first) {
                value.append(";");
            }
            Timestamp ts = entry.getValue();
            value.append(OptionsUtils.bytesToHex(entry.getKey().getBytes()));
            value.append(":");
            value.append(OptionsUtils.longToString(ts.startTimestamp));
            value.append(":");
            value.append(OptionsUtils.booleanToString(ts.startInclusive));
            value.append(":");
            value.append(OptionsUtils.longToString(ts.endTimestamp));
            value.append(":");
            value.append(OptionsUtils.booleanToString(ts.endInclusive));
            first = false;
        }
        iteratorSetting.addOption(TIMESTAMPS, value.toString());
    }

    @Override
    public boolean accept(Key key, Value value) {
        long ts = key.getTimestamp();
        Timestamp timestamp = timestamps.get(key.getRow());
        if (timestamp == null) {
            return true;
        }
        if (timestamp.startTimestamp != null) {
            if (timestamp.startInclusive) {
                if (ts < timestamp.startTimestamp) {
                    return false;
                }
            } else {
                if (ts <= timestamp.startTimestamp) {
                    return false;
                }
            }
        }
        if (timestamp.endTimestamp != null) {
            if (timestamp.endInclusive) {
                if (ts > timestamp.endTimestamp) {
                    return false;
                }
            } else {
                if (ts >= timestamp.endTimestamp) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        RowTimestampFilter copy = (RowTimestampFilter) super.deepCopy(env);
        copy.timestamps = timestamps;
        return copy;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        if (options == null) {
            throw new IllegalArgumentException(TIMESTAMPS + " is required");
        }

        super.init(source, options, env);

        String timestampOptions = options.get(TIMESTAMPS);
        if (timestampOptions == null) {
            throw new IllegalArgumentException(TIMESTAMPS + " is required");
        }
        timestamps = new HashMap<>();
        for (String timestampOption : timestampOptions.split(";")) {
            String[] parts = timestampOption.split(":");
            if (parts.length != 5) {
                throw new IllegalArgumentException(TIMESTAMPS + " is invalid. Expected 5 parts found " + parts.length + ": " + timestampOption);
            }
            Text rowKey = new Text(OptionsUtils.hexToBytes(parts[0]));
            Long startTimestamp = OptionsUtils.parseLong(parts[1]);
            Boolean startInclusive = OptionsUtils.parseBoolean(parts[2]);
            Long endTimestamp = OptionsUtils.parseLong(parts[3]);
            Boolean endInclusive = OptionsUtils.parseBoolean(parts[4]);
            timestamps.put(rowKey, new Timestamp(startTimestamp, startInclusive, endTimestamp, endInclusive));
        }
    }

    public static class Timestamp {
        private final Long startTimestamp;
        private final Boolean startInclusive;
        private final Long endTimestamp;
        private final Boolean endInclusive;

        public Timestamp(Long startTimestamp, Boolean startInclusive, Long endTimestamp, Boolean endInclusive) {
            if (startTimestamp != null && startInclusive == null) {
                throw new IllegalArgumentException("If startTimestamp is specified, startInclusive must also be specified");
            }
            if (startInclusive != null && startTimestamp == null) {
                throw new IllegalArgumentException("If startInclusive is specified, startTimestamp must also be specified");
            }
            if (endTimestamp != null && endInclusive == null) {
                throw new IllegalArgumentException("If endTimestamp is specified, endInclusive must also be specified");
            }
            if (endInclusive != null && endTimestamp == null) {
                throw new IllegalArgumentException("If endInclusive is specified, endTimestamp must also be specified");
            }
            this.startTimestamp = startTimestamp;
            this.startInclusive = startInclusive;
            this.endTimestamp = endTimestamp;
            this.endInclusive = endInclusive;
        }
    }
}

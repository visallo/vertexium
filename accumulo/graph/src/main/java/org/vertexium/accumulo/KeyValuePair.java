package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class KeyValuePair implements Comparable<KeyValuePair> {
    private final Key key;
    private final Value value;

    public KeyValuePair(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public Key getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(KeyValuePair o) {
        return getKey().compareTo(o.getKey());
    }
}

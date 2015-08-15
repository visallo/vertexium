package org.vertexium.sql.collections;

import com.google.common.base.Objects;

import java.util.Map;

class MapEntry<T> implements Map.Entry<String, T> {
    private String key;
    private T value;

    protected MapEntry(String key, T value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public T setValue(T value) {
        this.value = value;
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapEntry that = (MapEntry) o;
        return Objects.equal(key, that.key) && Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value);
    }
}

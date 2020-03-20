package org.vertexium.accumulo.iterator.model;

import org.vertexium.accumulo.iterator.util.ArrayUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EdgeLabels {
    private final List<byte[]> labels;

    public EdgeLabels(List<byte[]> labels) {
        this.labels = labels;
    }

    public EdgeLabels() {
        this(new ArrayList<>());
    }

    public int add(byte[] bytes) {
        int result = indexOf(bytes);
        if (result >= 0) {
            return result;
        }

        result = labels.size();
        labels.add(bytes);
        return result;
    }

    public int add(byte[] bytes, int offset, int length) {
        int result = indexOf(bytes, offset, length);
        if (result >= 0) {
            return result;
        }

        result = labels.size();
        labels.add(Arrays.copyOfRange(bytes, offset, offset + length));
        return result;
    }

    public boolean contains(byte[] bytes) {
        return indexOf(bytes) >= 0;
    }

    public int indexOf(byte[] bytes) {
        return indexOf(bytes, 0, bytes.length);
    }

    public int indexOf(byte[] bytes, int offset, int length) {
        int i = 0;
        for (byte[] label : labels) {
            if (ArrayUtils.equals(label, bytes, offset, length)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public List<byte[]> getLabels() {
        return labels;
    }

    public byte[] get(int index) {
        return labels.get(index);
    }

    public EdgeLabels cloneEdgeLabels() {
        return new EdgeLabels(new ArrayList<>(labels));
    }

    @Override
    public String toString() {
        return String.format(
            "EdgeLabels{labels=%s}",
            labels.stream()
                .map(arr -> new String(arr, StandardCharsets.UTF_8))
                .collect(Collectors.joining(", "))
        );
    }
}

package org.vertexium;

import org.vertexium.util.ArrayIterable;

import java.util.Arrays;
import java.util.Iterator;

public class Path implements Iterable<String> {
    private final String[] vertexIds;

    public Path(String... vertexIds) {
        this.vertexIds = vertexIds;
    }

    public Path(Path path, String vertexId) {
        this.vertexIds = Arrays.copyOf(path.vertexIds, path.vertexIds.length + 1);
        this.vertexIds[this.vertexIds.length - 1] = vertexId;
    }

    public int length() {
        return this.vertexIds.length;
    }

    public String get(int i) {
        return this.vertexIds[i];
    }

    @Override
    public Iterator<String> iterator() {
        return new ArrayIterable<String>(this.vertexIds).iterator();
    }

    @Override
    public String toString() {
        return Arrays.toString(vertexIds);
    }
}

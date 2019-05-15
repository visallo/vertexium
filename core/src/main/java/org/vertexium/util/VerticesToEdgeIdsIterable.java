package org.vertexium.util;

import org.vertexium.Direction;
import org.vertexium.User;
import org.vertexium.Vertex;

import java.util.Iterator;

import static org.vertexium.util.StreamUtils.stream;

public class VerticesToEdgeIdsIterable implements Iterable<String> {
    private final Iterable<? extends Vertex> vertices;
    private final User user;

    public VerticesToEdgeIdsIterable(Iterable<? extends Vertex> vertices, User user) {
        this.vertices = vertices;
        this.user = user;
    }

    @Override
    public Iterator<String> iterator() {
        return stream(this.vertices)
            .flatMap(vertex -> vertex.getEdgeIds(Direction.BOTH, user))
            .iterator();
    }
}

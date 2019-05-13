package org.vertexium.util;

import org.vertexium.Authorizations;
import org.vertexium.Direction;
import org.vertexium.User;
import org.vertexium.Vertex;

import java.util.Iterator;

import static org.vertexium.util.StreamUtils.toIterable;

public class VerticesToEdgeIdsIterable implements Iterable<String> {
    private final Iterable<? extends Vertex> vertices;
    private final User user;

    public VerticesToEdgeIdsIterable(Iterable<? extends Vertex> vertices, User user) {
        this.vertices = vertices;
        this.user = user;
    }

    @Override
    public Iterator<String> iterator() {
        return new SelectManyIterable<Vertex, String>(this.vertices) {
            @Override
            public Iterable<String> getIterable(Vertex vertex) {
                return toIterable(vertex.getEdgeIds(Direction.BOTH, user));
            }
        }.iterator();
    }
}

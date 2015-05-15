package org.vertexium.util;

import org.vertexium.Authorizations;
import org.vertexium.Direction;
import org.vertexium.Vertex;

import java.util.Iterator;

public class VerticesToEdgeIdsIterable implements Iterable<String> {
    private final Iterable<? extends Vertex> vertices;
    private final Authorizations authorizations;

    public VerticesToEdgeIdsIterable(Iterable<? extends Vertex> vertices, Authorizations authorizations) {
        this.vertices = vertices;
        this.authorizations = authorizations;
    }

    @Override
    public Iterator<String> iterator() {
        return new SelectManyIterable<Vertex, String>(this.vertices) {
            @Override
            public Iterable<String> getIterable(Vertex vertex) {
                return vertex.getEdgeIds(Direction.BOTH, authorizations);
            }
        }.iterator();
    }
}

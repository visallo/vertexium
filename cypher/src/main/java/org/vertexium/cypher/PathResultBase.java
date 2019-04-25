package org.vertexium.cypher;

import com.google.common.collect.Lists;
import org.vertexium.Edge;
import org.vertexium.Element;
import org.vertexium.Vertex;
import org.vertexium.cypher.exceptions.VertexiumCypherException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class PathResultBase {
    private final List<Element> elements;

    public PathResultBase(Element... elements) {
        this.elements = Lists.newArrayList(elements);
    }

    public PathResultBase(List<Element> elements) {
        this.elements = elements;
    }

    public PathResultBase(PathResultBase path, Element... additionalElements) {
        this.elements = new ArrayList<>();
        elements.addAll(path.elements);
        Collections.addAll(elements, additionalElements);
    }

    public Stream<Element> getElements() {
        return elements.stream();
    }

    public Stream<Edge> getEdges() {
        return getElements()
            .filter(e -> e instanceof Edge)
            .map(e -> (Edge) e);
    }

    public Stream<Vertex> getVertices() {
        return getElements()
            .filter(e -> e instanceof Vertex)
            .map(e -> (Vertex) e);
    }

    public int getLength() {
        return (int) getElements()
            .filter(e -> e instanceof Edge)
            .count();
    }

    public Element getTailElement() {
        if (elements.size() == 0) {
            return null;
        }
        return elements.get(elements.size() - 1);
    }

    public Vertex getLastVertex() {
        for (int i = elements.size() - 1; i >= 0; i--) {
            if (elements.get(i) instanceof Vertex) {
                return (Vertex) elements.get(i);
            }
        }
        return null;
    }

    public String getOtherVertexId(String vertexId) {
        if (elements.size() == 1 && elements.get(0) instanceof Edge) {
            Edge edge = (Edge) elements.get(0);
            return edge.getOtherVertexId(vertexId);
        }

        String headVertexId = getHeadVertexId();
        String tailVertexId = getTailVertexId();
        if (vertexId.equals(headVertexId)) {
            return tailVertexId;
        }
        if (vertexId.equals(tailVertexId)) {
            return headVertexId;
        }
        return null;
    }

    private String getHeadVertexId() {
        if (elements.size() == 0) {
            return null;
        }
        if (elements.get(0) instanceof Vertex) {
            return elements.get(0).getId();
        }
        if (elements.size() > 1) {
            Edge edge = (Edge) elements.get(0);
            Vertex next = (Vertex) elements.get(1);
            return edge.getOtherVertexId(next.getId());
        }
        throw new VertexiumCypherException("Could not get head vertex id");
    }

    private String getTailVertexId() {
        if (elements.size() == 0) {
            return null;
        }
        if (elements.get(elements.size() - 1) instanceof Vertex) {
            return elements.get(elements.size() - 1).getId();
        }
        if (elements.size() > 1) {
            Edge edge = (Edge) elements.get(elements.size() - 1);
            Vertex next = (Vertex) elements.get(elements.size() - 2);
            return edge.getOtherVertexId(next.getId());
        }
        throw new VertexiumCypherException("Could not get tail vertex id");
    }

    public boolean containsVertexId(String vertexId) {
        return getVertices()
            .anyMatch(v -> v.getId().equals(vertexId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PathResultBase that = (PathResultBase) o;
        return elements.equals(that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }
}

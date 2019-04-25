package org.vertexium;

import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.IterableUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EdgeVertexPair {
    private final Edge edge;
    private final Vertex vertex;

    public EdgeVertexPair(Edge edge, Vertex vertex) {
        this.edge = edge;
        this.vertex = vertex;
    }

    public Edge getEdge() {
        return edge;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return "EdgeVertexPair{" +
            "edge=" + edge +
            ", vertex=" + vertex +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EdgeVertexPair that = (EdgeVertexPair) o;

        if (!edge.equals(that.edge)) {
            return false;
        }
        if (!vertex.equals(that.vertex)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = edge.hashCode();
        result = 31 * result + vertex.hashCode();
        return result;
    }

    public static Iterable<EdgeVertexPair> getEdgeVertexPairs(
        Graph graph,
        String sourceVertexId,
        Iterable<EdgeInfo> edgeInfos,
        FetchHints fetchHints,
        Long endTime,
        Authorizations authorizations
    ) {
        Set<String> edgeIdsToFetch = new HashSet<>();
        Set<String> vertexIdsToFetch = new HashSet<>();
        for (EdgeInfo edgeInfo : edgeInfos) {
            edgeIdsToFetch.add(edgeInfo.getEdgeId());
            vertexIdsToFetch.add(edgeInfo.getVertexId());
        }
        final Map<String, Vertex> vertices = IterableUtils.toMapById(graph.getVertices(vertexIdsToFetch, fetchHints, endTime, authorizations));
        Iterable<Edge> edges = graph.getEdges(edgeIdsToFetch, fetchHints, endTime, authorizations);
        return new ConvertingIterable<Edge, EdgeVertexPair>(edges) {
            @Override
            protected EdgeVertexPair convert(Edge edge) {
                String otherVertexId = edge.getOtherVertexId(sourceVertexId);
                Vertex otherVertex = vertices.get(otherVertexId);
                if (otherVertex == null) {
                    throw new VertexiumException("Found an edge " + edge.getId() + ", but could not find the vertex on the other end: " + otherVertexId);
                }
                return new EdgeVertexPair(edge, otherVertex);
            }
        };
    }
}

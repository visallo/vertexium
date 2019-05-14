package org.vertexium;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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

    public static Stream<EdgeVertexPair> getEdgeVertexPairs(
        Graph graph,
        String sourceVertexId,
        Stream<EdgeInfo> edgeInfos,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        Set<String> edgeIdsToFetch = new HashSet<>();
        Set<String> vertexIdsToFetch = new HashSet<>();
        edgeInfos.forEach(edgeInfo -> {
            edgeIdsToFetch.add(edgeInfo.getEdgeId());
            vertexIdsToFetch.add(edgeInfo.getVertexId());
        });
        Map<String, Vertex> vertices = graph.getVerticesMappedById(vertexIdsToFetch, fetchHints, endTime, user);
        return graph.getEdges(edgeIdsToFetch, fetchHints, endTime, user)
            .filter(edge -> vertices.get(edge.getOtherVertexId(sourceVertexId)) != null)
            .map(edge -> {
                String otherVertexId = edge.getOtherVertexId(sourceVertexId);
                Vertex otherVertex = vertices.get(otherVertexId);
                if (otherVertex == null) {
                    throw new VertexiumException("Found an edge " + edge.getId() + ", but could not find the vertex on the other end: " + otherVertexId);
                }
                return new EdgeVertexPair(edge, otherVertex);
            });
    }
}

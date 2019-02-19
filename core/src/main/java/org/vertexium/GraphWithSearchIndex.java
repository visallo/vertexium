package org.vertexium;

import org.vertexium.search.SearchIndex;

public interface GraphWithSearchIndex extends Graph {
    SearchIndex getSearchIndex();

    /**
     * This method will only flush the primary graph and not the search index
     */
    void flushGraph();

    Iterable<Edge> addEdgesAsyncIndex(Iterable<ElementBuilder<Edge>> edges, Authorizations authorizations);

    Iterable<Vertex> addVerticesAsyncIndex(Iterable<ElementBuilder<Vertex>> vertices, Authorizations authorizations);
}

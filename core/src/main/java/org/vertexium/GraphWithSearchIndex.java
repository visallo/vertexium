package org.vertexium;

import org.vertexium.search.SearchIndex;

public interface GraphWithSearchIndex extends Graph {
    SearchIndex getSearchIndex();

    /**
     * This method will only flush the primary graph and not the search index
     */
    void flushGraph();
}

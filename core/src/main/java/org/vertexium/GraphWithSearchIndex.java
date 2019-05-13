package org.vertexium;

import org.vertexium.search.SearchIndex;

/**
 * @deprecated This interface will be merged with {@link Graph} in future versions of Vertexium.
 */
@Deprecated
public interface GraphWithSearchIndex extends Graph {
    SearchIndex getSearchIndex();

    /**
     * This method will only flush the primary graph and not the search index
     */
    void flushGraph();
}

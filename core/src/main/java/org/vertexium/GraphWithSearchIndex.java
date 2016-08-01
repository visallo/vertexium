package org.vertexium;

import org.vertexium.search.SearchIndex;

public interface GraphWithSearchIndex extends Graph {
    SearchIndex getSearchIndex();
}

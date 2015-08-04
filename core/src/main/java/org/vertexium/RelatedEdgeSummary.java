package org.vertexium;

import java.util.Collection;
import java.util.Map;

public interface RelatedEdgeSummary {
    Map<String, Collection<RelatedEdge>> getRelatedEdgesByLabel();
}

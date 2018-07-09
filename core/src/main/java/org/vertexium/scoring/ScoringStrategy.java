package org.vertexium.scoring;

import org.vertexium.VertexiumObject;

public interface ScoringStrategy {
    Double getScore(VertexiumObject vertexiumObject);
}

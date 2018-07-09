package org.vertexium.scoring;

import org.vertexium.VertexiumObject;

public class FieldValueScoringStrategy implements ScoringStrategy {
    private final String field;

    public FieldValueScoringStrategy(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    @Override
    public Double getScore(VertexiumObject vertexiumObject) {
        Iterable<Object> values = vertexiumObject.getPropertyValues(getField());
        double score = 0.0;
        for (Object value : values) {
            if (value instanceof Number) {
                score = Math.max(score, ((Number) value).doubleValue());
            }
        }
        return score;
    }
}

package org.vertexium.scoring;

import org.vertexium.VertexiumObject;

import java.math.BigInteger;

public class HammingDistanceScoringStrategy implements ScoringStrategy {
    private final String hash;
    private final String field;

    public HammingDistanceScoringStrategy(String field, String hash) {
        this.field = field;
        this.hash = hash;
    }

    public String getHash() {
        return hash;
    }

    public String getField() {
        return field;
    }

    @Override
    public Double getScore(VertexiumObject vertexiumObject) {
        double score = 0.0;
        Iterable<Object> values = vertexiumObject.getPropertyValues(getField());
        for (Object value : values) {
            if (value instanceof String) {
                String valueStr = (String) value;
                int maxLen = Math.min(valueStr.length(), hash.length()) * 4;
                score = Math.max(
                        score,
                        maxLen - new BigInteger(valueStr, 16).xor(new BigInteger(hash, 16)).bitCount()
                );
            }
        }
        return score;
    }
}

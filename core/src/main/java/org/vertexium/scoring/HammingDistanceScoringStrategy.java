package org.vertexium.scoring;

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
}

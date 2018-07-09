package org.vertexium.query;

import org.vertexium.VertexiumObject;
import org.vertexium.scoring.ScoringStrategy;

import java.util.Comparator;

public class ScoringStrategyComparator<T> implements Comparator<T> {
    private final ScoringStrategy scoringStrategy;

    public ScoringStrategyComparator(ScoringStrategy scoringStrategy) {
        this.scoringStrategy = scoringStrategy;
    }

    @Override
    public int compare(T o1, T o2) {
        if (o1 instanceof VertexiumObject && o2 instanceof VertexiumObject) {
            Double o1Score = scoringStrategy.getScore((VertexiumObject) o1);
            Double o2Score = scoringStrategy.getScore((VertexiumObject) o2);
            if (o1Score == null && o2Score == null) {
                return 0;
            }
            if (o1Score != null && o2Score == null) {
                return 1;
            }
            if (o1Score == null && o2Score != null) {
                return -1;
            }
            return -o1Score.compareTo(o2Score);
        }
        return 0;
    }
}

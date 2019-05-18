package org.vertexium.query;

import java.util.Collection;

public class StatisticsResult extends AggregationResult {
    private final long count;
    private final double sum;
    private final double min;
    private final double max;
    private final double standardDeviation;

    public StatisticsResult(long count, double sum, double min, double max, double standardDeviation) {
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.standardDeviation = standardDeviation;
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAverage() {
        if (getCount() == 0) {
            return 0.0;
        }
        return getSum() / (double) getCount();
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public static StatisticsResult combine(Collection<StatisticsResult> statisticsResults) {
        long count = 0;
        double sum = 0.0;
        double min = 0.0;
        double max = 0.0;
        boolean first = true;
        for (StatisticsResult statisticsResult : statisticsResults) {
            count += statisticsResult.getCount();
            sum += statisticsResult.getSum();
            if (first) {
                min = statisticsResult.getMin();
                max = statisticsResult.getMax();
            } else {
                min = Math.min(min, statisticsResult.getMin());
                max = Math.max(max, statisticsResult.getMax());
            }
            first = false;
        }

        double average = count == 0 ? 0.0 : sum / (double) count;

        double standardDeviationS1 = 0.0;
        double standardDeviationS2 = 0.0;
        for (StatisticsResult statisticsResult : statisticsResults) {
            if (statisticsResult.getCount() == 0) {
                continue;
            }
            standardDeviationS1 += statisticsResult.getCount() * Math.pow(statisticsResult.getStandardDeviation(), 2.0);
            standardDeviationS2 += statisticsResult.getCount() * Math.pow(statisticsResult.getAverage() - average, 2.0);
        }
        double variance = (standardDeviationS1 + standardDeviationS2) / (double) count;
        double standardDeviation = Math.sqrt(variance);

        return new StatisticsResult(count, sum, min, max, standardDeviation);
    }
}

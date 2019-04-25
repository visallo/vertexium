package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;

import java.util.List;

public class PercentileContFunction extends PercentileFunction {
    @Override
    protected Object invoke(VertexiumCypherQueryContext ctx, List<Double> values, double percentile) {
        int count = values.size();
        values.sort(Double::compare);

        if (percentile == 1.0 || count == 1) {
            return values.get(values.size() - 1);
        } else if (count > 1) {
            double doubleIndex = percentile * (double) (count - 1);
            int floor = (int) doubleIndex;
            int ceil = (int) Math.ceil(doubleIndex);
            if (ceil == floor || floor == count - 1) {
                return values.get(floor);
            } else {
                return values.get(floor) * (ceil - doubleIndex) + values.get(ceil) * (doubleIndex - floor);
            }
        } else {
            return null;
        }
    }
}

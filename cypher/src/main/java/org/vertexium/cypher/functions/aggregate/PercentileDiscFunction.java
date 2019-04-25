package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;

import java.util.List;

public class PercentileDiscFunction extends PercentileFunction {
    @Override
    protected Object invoke(VertexiumCypherQueryContext ctx, List<Double> values, double percentile) {
        int count = values.size();
        values.sort(Double::compare);

        if (percentile == 1.0 || count == 1) {
            return values.get(values.size() - 1);
        } else if (count > 1) {
            double doubleIndex = percentile * (double) count;
            int index = (int) doubleIndex;
            index = (doubleIndex != index || index == 0) ? index : index - 1;
            return values.get(index);
        } else {
            return null;
        }
    }
}

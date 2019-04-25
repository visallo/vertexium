package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

import java.util.HashMap;
import java.util.Map;

public class MapLiteralExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final String[] keys;

    public MapLiteralExecutionStep(String resultName, ExecutionStepWithResultName[] entries) {
        super(entries);
        this.resultName = resultName;
        this.keys = new String[entries.length];
        for (int i = 0; i < entries.length; i++) {
            this.keys[i] = entries[i].getResultName();
        }
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Map<String, Object> map = new HashMap<>();
            for (String key : this.keys) {
                map.put(key, row.get(key));
            }
            row.popScope(keys.length);
            row.pushScope(getResultName(), map);
        });
    }

    @Override
    public String toString() {
        return String.format("%s {resultName='%s'}", super.toString(), resultName);
    }
}

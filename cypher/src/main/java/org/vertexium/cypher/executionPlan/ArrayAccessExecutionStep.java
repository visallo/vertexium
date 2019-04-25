package org.vertexium.cypher.executionPlan;

import org.vertexium.Element;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

import java.lang.reflect.Array;
import java.util.Map;

public class ArrayAccessExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final String arrayResultName;
    private final String indexResultName;

    public ArrayAccessExecutionStep(String resultName, ExecutionStepWithResultName arr, ExecutionStepWithResultName index) {
        super(arr, index);
        this.resultName = resultName;
        this.arrayResultName = arr.getResultName();
        this.indexResultName = index.getResultName();
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object arr = row.get(arrayResultName);
            if (arr == null) {
                throw new VertexiumCypherException("Could not get array");
            }
            Object indexObject = row.get(indexResultName);

            Object value;
            if (arr.getClass().isArray()) {
                value = executeOnArray(arr, indexObject);
            } else if (arr instanceof Element) {
                value = executeOnElement((Element) arr, indexObject);
            } else if (arr instanceof Map) {
                value = executeOnMap((Map) arr, indexObject);
            } else {
                throw new VertexiumCypherNotImplemented("expected array or element, found " + arr.getClass().getName());
            }
            row.pushScope(getResultName(), value);
        });
    }

    private Object executeOnMap(Map map, Object indexObject) {
        if (!(indexObject instanceof String)) {
            throw new VertexiumCypherNotImplemented("element index must be a string");
        }

        return map.get(indexObject);
    }

    private Object executeOnElement(Element element, Object indexObject) {
        if (!(indexObject instanceof String)) {
            throw new VertexiumCypherNotImplemented("element index must be a string");
        }

        String index = (String) indexObject;
        return element.getPropertyValue(index);
    }

    private Object executeOnArray(Object arr, Object indexObject) {
        if (!(indexObject instanceof Number)) {
            throw new VertexiumCypherNotImplemented("array index must be a number");
        }

        int index = ((Number) indexObject).intValue();

        return Array.get(arr, index);
    }
}

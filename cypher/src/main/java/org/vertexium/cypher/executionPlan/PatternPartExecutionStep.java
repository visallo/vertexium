package org.vertexium.cypher.executionPlan;

import org.vertexium.Element;
import org.vertexium.cypher.PathResult;
import org.vertexium.cypher.PathResultBase;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

public class PatternPartExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final List<String> pathResultNames;

    public PatternPartExecutionStep(String resultName, MatchPartExecutionStep... matchPartExecutionSteps) {
        super(matchPartExecutionSteps);
        this.resultName = resultName;
        this.pathResultNames = stream(matchPartExecutionSteps)
            .map(MatchPartExecutionStep::getResultName)
            .collect(Collectors.toList());
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        if (resultName != null) {
            source = source.peek(row -> {
                PathResultBase pathResult = new PathResult(
                    pathResultNames.stream()
                        .flatMap(name -> {
                            Object o = row.get(name);
                            if (o == null) {
                                return Stream.of((Element) null);
                            } else if (o instanceof Element) {
                                return Stream.of((Element) o);
                            } else if (o instanceof PathResultBase) {
                                return ((PathResultBase) o).getElements();
                            } else {
                                throw new VertexiumCypherNotImplemented("Unhandled value: " + o.getClass().getName());
                            }
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList())
                );
                row.set(resultName, pathResult);
            });
        }
        return source;
    }

    @Override
    public String toString() {
        return String.format("%s {resultName=%s}", super.toString(), resultName);
    }
}

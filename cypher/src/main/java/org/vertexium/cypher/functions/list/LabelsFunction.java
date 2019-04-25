package org.vertexium.cypher.functions.list;

import com.google.common.collect.Lists;
import org.vertexium.Edge;
import org.vertexium.Vertex;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class LabelsFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object item = arguments[0];
        if (item == null) {
            throw new VertexiumException("Could not find Vertex using");
        }

        if (item instanceof Vertex) {
            Vertex vertex = (Vertex) item;
            return ctx.getVertexLabels(vertex);
        }

        if (item instanceof Edge) {
            Edge edge = (Edge) item;
            return Lists.newArrayList(edge.getLabel());
        }

        throw new VertexiumCypherTypeErrorException(item, Vertex.class, Edge.class);
    }
}

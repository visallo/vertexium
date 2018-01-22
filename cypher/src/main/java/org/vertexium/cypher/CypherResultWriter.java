package org.vertexium.cypher;

import org.vertexium.Edge;
import org.vertexium.Element;
import org.vertexium.Property;
import org.vertexium.Vertex;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.StreamUtils.stream;

public class CypherResultWriter {
    public String columnValueToString(VertexiumCypherQueryContext ctx, Object o) {
        if (o == null) {
            return columnNullValueToString();
        } else if (o instanceof Optional) {
            return columnValueToString(ctx, ((Optional<?>) o).orElse(null));
        } else if (o instanceof Map) {
            //noinspection unchecked
            return columnMapValueToString(ctx, (Map<String, Object>) o);
        } else if (o instanceof Double) {
            return columnDoubleValueToString((double) o);
        } else if (o instanceof Vertex) {
            return columnVertexToString(ctx, (Vertex) o);
        } else if (o instanceof Edge) {
            return columnEdgeToString(ctx, (Edge) o);
        } else if (o instanceof String) {
            return columnStringToString(o);
        } else if (o instanceof VertexiumCypherScope.PathItem) {
            return columnPathResultToString(ctx, (VertexiumCypherScope.PathItem) o);
        } else if (o instanceof Stream) {
            return columnValueIterableToString(ctx, ((Stream<?>) o).collect(Collectors.toList()));
        } else if (o instanceof Iterable) {
            return columnValueIterableToString(ctx, (Iterable<?>) o);
        } else {
            return columnUnknownToString(o);
        }
    }

    private String columnPathResultToString(VertexiumCypherQueryContext ctx, VertexiumCypherScope.PathItem pathResult) {
        return pathResult.toString(ctx);
    }

    private String columnValueIterableToString(VertexiumCypherQueryContext ctx, Iterable<?> list) {
        return "[" + stream(list).map(item -> columnValueToString(ctx, item)).collect(Collectors.joining(", ")) + "]";
    }

    private String columnUnknownToString(Object o) {
        return o.toString();
    }

    private String columnStringToString(Object o) {
        return "'" + o + "'";
    }

    private String columnVertexToString(VertexiumCypherQueryContext ctx, Vertex vertex) {
        StringBuilder result = new StringBuilder();
        result.append("(");
        int propertyCount = 0;
        for (Property property : vertex.getProperties()) {
            if (property.getName().equals(ctx.getLabelPropertyName())) {
                result.append(":");
                result.append(property.getValue());
            } else {
                propertyCount++;
            }
        }
        if (propertyCount > 0) {
            if (result.length() > "(".length()) {
                result.append(" ");
            }
            result.append(elementPropertiesToString(ctx, vertex));
        }
        result.append(")");
        return result.toString();
    }

    private String elementPropertiesToString(VertexiumCypherQueryContext ctx, Element element) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (Property property : element.getProperties()) {
            if (property.getName().equals(ctx.getLabelPropertyName())) {
                continue;
            }
            if (first) {
                result.append("{");
            } else {
                result.append(", ");
            }
            result.append(property.getName());
            result.append(": ");
            result.append(columnValueToString(ctx, property.getValue()));
            first = false;
        }
        if (result.length() > 0) {
            result.append("}");
        }
        return result.toString();
    }

    private String columnEdgeToString(VertexiumCypherQueryContext ctx, Edge edge) {
        StringBuilder result = new StringBuilder();
        result.append("[");
        result.append(":");
        result.append(edge.getLabel());
        if (count(edge.getProperties()) > 0) {
            result.append(" ");
            result.append(elementPropertiesToString(ctx, edge));
        }
        result.append("]");
        return result.toString();
    }

    private String columnDoubleValueToString(double o) {
        NumberFormat formatter = new DecimalFormat(o < 0 ? ".0#############" : "0.0#############");
        return formatter.format(o);
    }

    private String columnMapValueToString(VertexiumCypherQueryContext ctx, Map<String, Object> o) {
        StringBuilder result = new StringBuilder();
        result.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : o.entrySet()) {
            if (!first) {
                result.append(", ");
            }
            result.append(entry.getKey());
            result.append(": ");
            result.append(columnValueToString(ctx, entry.getValue()));
            first = false;
        }
        result.append("}");
        return result.toString();
    }

    private String columnNullValueToString() {
        return "null";
    }
}

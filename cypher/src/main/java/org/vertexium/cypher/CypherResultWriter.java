package org.vertexium.cypher;

import org.vertexium.*;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
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
        } else if (o instanceof CypherDuration) {
            return columnCypherDurationToString((CypherDuration) o);
        } else if (o instanceof PathResultBase) {
            return columnPathResultToString(ctx, (PathResultBase) o);
        } else if (o instanceof Stream) {
            return columnValueIterableToString(ctx, (Stream<?>) o);
        } else if (o instanceof Iterable) {
            return columnValueIterableToString(ctx, stream((Iterable<?>) o));
        } else if (o.getClass().isArray()) {
            return columnValueIterableToString(ctx, Arrays.stream((Object[]) o));
        } else {
            return columnUnknownToString(o);
        }
    }

    private String columnCypherDurationToString(CypherDuration duration) {
        return "'" + duration + "'";
    }

    private String columnPathResultToString(VertexiumCypherQueryContext ctx, PathResultBase pathResult) {
        if (pathResult instanceof RelationshipRangePathResult) {
            return String.format(
                "[%s]",
                pathResult.getEdges()
                    .map(e -> columnValueToString(ctx, e))
                    .collect(Collectors.joining(", "))
            );
        }

        StringBuilder result = new StringBuilder();
        result.append("<");
        AtomicReference<Vertex> previousVertex = new AtomicReference<>();
        AtomicReference<Element> previousElement = new AtomicReference<>();
        pathResult.getElements().forEach(element -> {
            if (element == null) {
                // do nothing
            } else if (element instanceof Edge) {
                Edge edge = (Edge) element;
                Direction direction = null;
                if (previousVertex.get() != null) {
                    direction = getDirection(previousVertex.get().getId(), edge);
                }
                if (direction == Direction.IN) {
                    result.append("<");
                }
                result.append("-");
                result.append(columnValueToString(ctx, element));
                result.append("-");
                if (direction == Direction.OUT) {
                    result.append(">");
                }
            } else if (element instanceof Vertex) {
                Vertex vertex = (Vertex) element;
                if (previousElement.get() == null && vertex.equals(previousVertex.get())) {
                    // this is a result of a zero length path
                } else {
                    result.append(columnValueToString(ctx, element));
                    previousVertex.set(vertex);
                }
            } else {
                throw new VertexiumException("unexpected element type: " + element.getClass().getName());
            }
            previousElement.set(element);
        });
        result.append(">");
        return result.toString();
    }

    private Direction getDirection(String previousVertexId, Edge element) {
        if (element.getVertexId(Direction.OUT).equals(previousVertexId)) {
            return Direction.OUT;
        } else {
            return Direction.IN;
        }
    }

    private String columnValueIterableToString(VertexiumCypherQueryContext ctx, Stream<?> list) {
        return "[" + list.map(item -> columnValueToString(ctx, item)).collect(Collectors.joining(", ")) + "]";
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

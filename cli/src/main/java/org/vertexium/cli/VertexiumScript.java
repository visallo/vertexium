package org.vertexium.cli;

import groovy.lang.Script;
import org.apache.commons.io.IOUtils;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.vertexium.*;
import org.vertexium.cli.model.LazyEdge;
import org.vertexium.cli.model.LazyExtendedDataTable;
import org.vertexium.cli.model.LazyProperty;
import org.vertexium.cli.model.LazyVertex;
import org.vertexium.cli.utils.ShellTableWriter;
import org.vertexium.cypher.VertexiumCypherQuery;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.property.StreamingPropertyValue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class VertexiumScript extends Script {
    private static Graph graph;
    private static Authorizations authorizations;
    private static final Map<String, LazyExtendedDataTable> contextExtendedDataTables = new HashMap<>();
    private static final Map<String, LazyProperty> contextProperties = new HashMap<>();
    private static final Map<String, LazyEdge> contextEdges = new HashMap<>();
    private static final Map<String, LazyVertex> contextVertices = new HashMap<>();
    private static Long time;

    public static void setGraph(Graph graph) {
        VertexiumScript.graph = graph;
    }

    public static Graph getGraph() {
        return graph;
    }

    public static void setTime(Long time) {
        VertexiumScript.time = time;
    }

    public static Long getTime() {
        return time;
    }

    @Override
    public Object run() {
        return null;
    }

    @Override
    public Object getProperty(String property) {
        if ("q".equals(property)) {
            return getGraph().query(getAuthorizations());
        }

        if ("now".equals(property)) {
            return System.currentTimeMillis();
        }

        Object contextExtendedDataTable = contextExtendedDataTables.get(property);
        if (contextExtendedDataTable != null) {
            return contextExtendedDataTable;
        }

        Object contextProperty = contextProperties.get(property);
        if (contextProperty != null) {
            return contextProperty;
        }

        Object contextEdge = contextEdges.get(property);
        if (contextEdge != null) {
            return contextEdge;
        }

        Object contextVertex = contextVertices.get(property);
        if (contextVertex != null) {
            return contextVertex;
        }

        return super.getProperty(property);
    }

    public static Authorizations getAuthorizations() {
        if (authorizations == null) {
            authorizations = getGraph().createAuthorizations();
        }
        return authorizations;
    }

    public static void setAuthorizations(Authorizations authorizations) {
        VertexiumScript.authorizations = authorizations;
    }

    public static Map<String, LazyExtendedDataTable> getContextExtendedDataTables() {
        return contextExtendedDataTables;
    }

    public static Object executeCypher(String code) {
        VertexiumCypherQueryContext ctx = new CliVertexiumCypherQueryContext(getGraph(), getAuthorizations());
        CypherCompilerContext compilerContext = new CypherCompilerContext(ctx.getFunctions());
        VertexiumCypherQuery query = VertexiumCypherQuery.parse(compilerContext, code);
        return query.execute(ctx);
    }

    public static Map<String, LazyProperty> getContextProperties() {
        return contextProperties;
    }

    public static Map<String, LazyEdge> getContextEdges() {
        return contextEdges;
    }

    public static Map<String, LazyVertex> getContextVertices() {
        return contextVertices;
    }

    public static String valueToString(Object value, boolean expanded) {
        if (value == null) {
            return null;
        }
        if (expanded) {
            if (value instanceof StreamingPropertyValue) {
                value = convertStreamingPropertyValue(value);
            }
            if (value.getClass() == byte[].class) {
                value = valueBytesToStringExpanded((byte[]) value);
            } else {
                value = value.toString();
            }
            if (((String) value).indexOf('\n') >= 0) {
                value = "\n" + value;
            } else {
                value = " " + value;
            }
        } else if (value.getClass() == byte[].class) {
            value = valueBytesToString((byte[]) value, 20);
        }
        return value.toString();
    }

    private static String valueBytesToStringExpanded(byte[] value) {
        StringBuilder sb = new StringBuilder();
        int octetsPerLine = 16;
        for (int i = 0; i < value.length; i += octetsPerLine) {
            for (int col = 0; col < octetsPerLine; col++) {
                if (i + col < value.length) {
                    sb.append(toHexOctet(value[i + col])).append(" ");
                } else {
                    sb.append("   ");
                }
            }
            sb.append("  ");
            for (int col = 0; col < octetsPerLine; col++) {
                if (i + col < value.length) {
                    char c = (char) value[i + col];
                    if (c >= ' ' && c <= '~') {
                        sb.append(c);
                    } else {
                        sb.append('.');
                    }
                } else {
                    sb.append(' ');
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private static String valueBytesToString(byte[] valueAsBytes, int length) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        int i;
        for (i = 0; i < Math.min(valueAsBytes.length, length); i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append(toHexOctet(valueAsBytes[i]));
        }
        if (i < valueAsBytes.length) {
            sb.append("...");
        }
        sb.append(']');
        return sb.toString();
    }

    private static String toHexOctet(byte valueAsByte) {
        String s = "00" + Integer.toHexString(valueAsByte);
        return s.substring(s.length() - 2);
    }

    private static Object convertStreamingPropertyValue(Object value) {
        StreamingPropertyValue spv = (StreamingPropertyValue) value;
        try {
            if (spv.getValueType() == String.class) {
                try (InputStream in = spv.getInputStream()) {
                    value = IOUtils.toString(in, Charset.defaultCharset());
                }
            } else if (spv.getValueType() == byte[].class) {
                try (InputStream in = spv.getInputStream()) {
                    byte[] buffer = new byte[100000];
                    int read;
                    read = in.read(buffer, 0, buffer.length);
                    value = Arrays.copyOfRange(buffer, 0, read);
                }
            } else {
                return "Could not convert StreamingPropertyValue of type " + spv.getValueType().getName();
            }
        } catch (IOException e) {
            throw new SecurityException("Could not get StreamingPropertyValue input stream", e);
        }
        return value;
    }

    public static String resultToString(Object obj) {
        if (obj instanceof Vertex) {
            return LazyVertex.toString((Vertex) obj);
        }
        if (obj instanceof Edge) {
            return LazyEdge.toString((Edge) obj);
        }
        if (obj instanceof Property) {
            return LazyProperty.toString((Property) obj, "property");
        }
        if (obj instanceof VertexiumCypherResult) {
            return vertexiumCypherResultToString((VertexiumCypherResult) obj);
        }
        return InvokerHelper.toString(obj);
    }

    private static String vertexiumCypherResultToString(VertexiumCypherResult cypherResult) {
        VertexiumScript.getContextProperties().clear();
        AtomicInteger vertexIndex = new AtomicInteger(0);
        AtomicInteger edgeIndex = new AtomicInteger(0);

        VertexiumCypherQueryContext ctx = new CliVertexiumCypherQueryContext(getGraph(), getAuthorizations());
        LinkedHashSet<String> columnNames = cypherResult.getColumnNames();
        List<List<String>> table = new ArrayList<>();
        table.add(new ArrayList<>(columnNames));
        table.addAll(
            cypherResult
                .map(row -> columnNames.stream()
                    .map(columnName -> row.get(columnName))
                    .map(o -> {
                        if (o instanceof Vertex) {
                            String vertexIndexString = "v" + vertexIndex.get();
                            LazyVertex lazyVertex = new LazyVertex(((Vertex) o).getId());
                            VertexiumScript.getContextVertices().put(vertexIndexString, lazyVertex);
                            vertexIndex.incrementAndGet();
                            return "@|bold " + vertexIndexString + ":|@ " + ((Vertex) o).getId();
                        }
                        if (o instanceof Edge) {
                            String edgeIndexString = "e" + edgeIndex.get();
                            LazyEdge lazyEdge = new LazyEdge(((Edge) o).getId());
                            VertexiumScript.getContextEdges().put(edgeIndexString, lazyEdge);
                            edgeIndex.incrementAndGet();
                            return "@|bold " + edgeIndexString + ":|@ " + ((Edge) o).getId();
                        }
                        return ctx.getResultWriter().columnValueToString(ctx, o);
                    })
                    .collect(Collectors.toList()))
                .collect(Collectors.toList())
        );

        return "\n" + ShellTableWriter.tableToString(table);
    }

    public static String getTimeString() {
        return getTimeString(getTime());
    }

    public static String getTimeString(Long t) {
        if (t == null) {
            return null;
        }
        return new Date(t) + " (" + t + ")";
    }

    public static String timestampToString(long timestamp) {
        return new Date(timestamp) + " (" + timestamp + ")";
    }
}

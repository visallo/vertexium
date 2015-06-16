package org.vertexium.cli;

import groovy.lang.Script;
import org.apache.commons.io.IOUtils;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.vertexium.*;
import org.vertexium.cli.model.*;
import org.vertexium.property.StreamingPropertyValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class VertexiumScript extends Script {
    private static Graph graph;
    private static Authorizations authorizations;
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
        if ("v".equals(property)) {
            return new LazyVertexMap();
        }

        if ("e".equals(property)) {
            return new LazyEdgeMap();
        }

        if ("g".equals(property)) {
            return getGraph();
        }

        if ("auths".equals(property)) {
            return getAuthorizations();
        }

        if ("time".equals(property)) {
            return getTime();
        }

        if ("now".equals(property)) {
            return System.currentTimeMillis();
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
                    value = IOUtils.toString(in);
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
        return InvokerHelper.toString(obj);
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

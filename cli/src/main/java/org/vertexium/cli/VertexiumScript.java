package org.vertexium.cli;

import groovy.lang.Script;
import org.apache.commons.io.IOUtils;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.vertexium.*;
import org.vertexium.cli.model.*;
import org.vertexium.property.StreamingPropertyValue;

import java.io.IOException;
import java.io.InputStream;
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
                StreamingPropertyValue spv = (StreamingPropertyValue) value;
                if (spv.getValueType() == String.class) {
                    try {
                        try (InputStream in = spv.getInputStream()) {
                            return IOUtils.toString(in);
                        }
                    } catch (IOException e) {
                        throw new SecurityException("Could not get StreamingPropertyValue input stream", e);
                    }
                }
            }
        }
        return value.toString();
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
}

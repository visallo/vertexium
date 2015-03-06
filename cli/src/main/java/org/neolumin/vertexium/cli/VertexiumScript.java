package org.neolumin.vertexium.cli;

import groovy.lang.Script;
import org.apache.commons.io.IOUtils;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.neolumin.vertexium.*;
import org.neolumin.vertexium.cli.model.*;
import org.neolumin.vertexium.property.StreamingPropertyValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VertexiumScript extends Script {
    private static Graph graph;
    private static Authorizations authorizations;
    private static final Map<String, LazyProperty> contextProperties = new HashMap<>();
    private static final Map<String, LazyEdge> contextEdges = new HashMap<>();
    private static final Map<String, LazyVertex> contextVertices = new HashMap<>();

    public static void setGraph(Graph graph) {
        VertexiumScript.graph = graph;
    }

    public static Graph getGraph() {
        return graph;
    }

    @Override
    public Object run() {
        return null;
    }

    @Override
    public Object invokeMethod(String name, Object args) {
        if ("setauths".equalsIgnoreCase(name)) {
            return invokeSetAuths(args);
        }

        if ("getauths".equalsIgnoreCase(name)) {
            return invokeGetAuths();
        }

        return super.invokeMethod(name, args);
    }

    private Object invokeGetAuths() {
        return getAuthorizations();
    }

    private Object invokeSetAuths(Object args) {
        String[] auths = invokeMethodArgsToStrings(args);
        setAuthorizations(getGraph().createAuthorizations(auths));
        return invokeGetAuths();
    }

    private String[] invokeMethodArgsToStrings(Object args) {
        if (args == null) {
            return new String[0];
        }
        Object[] authsObjects = (Object[]) args;
        List<String> authsList = new ArrayList<>();
        for (Object authObject : authsObjects) {
            authsList.add(authObject.toString());
        }
        return authsList.toArray(new String[authsList.size()]);
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

        if ("setauths".equalsIgnoreCase(property)) {
            return invokeSetAuths(null);
        }

        if ("getauths".equalsIgnoreCase(property)) {
            return invokeGetAuths();
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
            return LazyVertex.toString((Vertex) obj, getAuthorizations());
        }
        if (obj instanceof Edge) {
            return LazyEdge.toString((Edge) obj);
        }
        if (obj instanceof Property) {
            return LazyProperty.toString((Property) obj, "property");
        }
        return InvokerHelper.toString(obj);
    }
}

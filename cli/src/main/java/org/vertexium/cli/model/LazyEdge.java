package org.vertexium.cli.model;

import org.vertexium.Direction;
import org.vertexium.Edge;
import org.vertexium.FetchHint;
import org.vertexium.Property;
import org.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LazyEdge extends ModelBase {
    private final String edgeId;

    public LazyEdge(String edgeId) {
        this.edgeId = edgeId;
    }

    @Override
    public String toString() {
        Edge e = getE();
        if (e == null) {
            return null;
        }

        return toString(e);
    }

    public static String toString(Edge e) {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);
        writer.println("@|bold " + e.getId() + "|@");
        writer.println("  @|bold visibility:|@ " + e.getVisibility());
        writer.println("  @|bold label:|@ " + e.getLabel());
        writer.println("  @|bold timestamp:|@ " + e.getTimestamp());

        writer.println("  @|bold properties:|@");
        VertexiumScript.getContextProperties().clear();
        int propIndex = 0;
        for (Property prop : e.getProperties()) {
            String propertyIndexString = "p" + propIndex;
            String valueString = VertexiumScript.valueToString(prop.getValue(), false);
            writer.println("    @|bold " + propertyIndexString + ":|@ " + prop.getName() + ":" + prop.getKey() + "[" + prop.getVisibility().getVisibilityString() + "] = " + valueString);
            LazyProperty lazyProperty = new LazyEdgeProperty(e.getId(), prop.getKey(), prop.getName(), prop.getVisibility());
            VertexiumScript.getContextProperties().put(propertyIndexString, lazyProperty);
            propIndex++;
        }

        VertexiumScript.getContextVertices().clear();
        int vertexIndex = 0;

        writer.println("  @|bold out vertex:|@");
        String vertexIndexString = "v" + vertexIndex;
        writer.println("    @|bold " + vertexIndexString + ":|@ " + e.getVertexId(Direction.OUT));
        LazyVertex lazyVertex = new LazyVertex(e.getVertexId(Direction.OUT));
        VertexiumScript.getContextVertices().put(vertexIndexString, lazyVertex);
        vertexIndex++;

        writer.println("  @|bold in vertex:|@");
        vertexIndexString = "v" + vertexIndex;
        writer.println("    @|bold " + vertexIndexString + ":|@ " + e.getVertexId(Direction.IN));
        lazyVertex = new LazyVertex(e.getVertexId(Direction.IN));
        VertexiumScript.getContextVertices().put(vertexIndexString, lazyVertex);
        vertexIndex++;

        return out.toString();
    }

    private Edge getE() {
        return getGraph().getEdge(getId(), FetchHint.ALL, getTime(), getAuthorizations());
    }

    public String getId() {
        return edgeId;
    }
}

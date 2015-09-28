package org.vertexium.cli.model;

import org.vertexium.*;
import org.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.vertexium.util.IterableUtils.toList;

public class LazyVertex extends ModelBase {
    private final String vertexId;

    public LazyVertex(String vertexId) {
        this.vertexId = vertexId;
    }

    @Override
    public String toString() {
        Vertex v = getV();
        if (v == null) {
            return null;
        }

        return toString(v);
    }

    public static String toString(Vertex v) {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);
        writer.println("@|bold " + v.getId() + "|@");
        writer.println("  @|bold hidden:|@ " + v.isHidden(getAuthorizations()));
        writer.println("  @|bold visibility:|@ " + v.getVisibility());
        writer.println("  @|bold timestamp:|@ " + VertexiumScript.timestampToString(v.getTimestamp()));

        writer.println("  @|bold hidden visibilities:|@");
        List<Visibility> hiddenVisibilities = toList(v.getHiddenVisibilities());
        if (hiddenVisibilities.size() == 0) {
            writer.println("    none");
        } else {
            for (Visibility hiddenVisibility : hiddenVisibilities) {
                writer.println("    " + hiddenVisibility.getVisibilityString());
            }
        }

        writer.println("  @|bold properties:|@");
        VertexiumScript.getContextProperties().clear();
        int propIndex = 0;
        for (Property prop : v.getProperties()) {
            String propertyIndexString = "p" + propIndex;
            String valueString = VertexiumScript.valueToString(prop.getValue(), false);
            boolean isHidden = prop.isHidden(getAuthorizations());
            writer.println(
                    "    @|bold " + propertyIndexString + ":|@ "
                            + prop.getName() + ":"
                            + prop.getKey()
                            + "[" + prop.getVisibility().getVisibilityString() + "] "
                            + "= " + valueString
                            + (isHidden ? " @|red (hidden)|@" : "")
            );
            LazyProperty lazyProperty = new LazyVertexProperty(v.getId(), prop.getKey(), prop.getName(), prop.getVisibility());
            VertexiumScript.getContextProperties().put(propertyIndexString, lazyProperty);
            propIndex++;
        }

        VertexiumScript.getContextEdges().clear();
        int edgeIndex = 0;

        writer.println("  @|bold out edges:|@");
        for (Edge edge : v.getEdges(Direction.OUT, FetchHint.ALL_INCLUDING_HIDDEN, getTime(), getAuthorizations())) {
            String edgeIndexString = "e" + edgeIndex;
            boolean isHidden = edge.isHidden(getAuthorizations());
            try {
                String otherVertexId = edge.getOtherVertexId(v.getId());
                writer.println(
                        "    @|bold " + edgeIndexString + ":|@ "
                                + edge.getId() + ": "
                                + edge.getLabel()
                                + " -> " + otherVertexId
                                + (isHidden ? " @|red (hidden)|@" : "")
                );
            } catch (Exception ex) {
                writer.println(
                        "    @|bold " + edgeIndexString + ":|@ "
                                + edge.getId() + ": "
                                + edge.getLabel()
                                + " -> @|red " + ex.getMessage() + "|@"
                                + (isHidden ? " @|red (hidden)|@" : "")
                );
            }
            LazyEdge lazyEdge = new LazyEdge(edge.getId());
            VertexiumScript.getContextEdges().put(edgeIndexString, lazyEdge);
            edgeIndex++;
        }

        writer.println("  @|bold in edges:|@");
        for (Edge edge : v.getEdges(Direction.IN, FetchHint.ALL_INCLUDING_HIDDEN, getTime(), getAuthorizations())) {
            String edgeIndexString = "e" + edgeIndex;
            boolean isHidden = edge.isHidden(getAuthorizations());
            try {
                String otherVertexId = edge.getOtherVertexId(v.getId());
                writer.println(
                        "    @|bold " + edgeIndexString + ":|@ "
                                + edge.getId() + ": "
                                + edge.getLabel()
                                + " -> " + otherVertexId
                                + (isHidden ? " @|red (hidden)|@" : "")
                );
            } catch (Exception ex) {
                writer.println(
                        "    @|bold " + edgeIndexString + ":|@ "
                                + edge.getId() + ": "
                                + edge.getLabel()
                                + " -> @|red " + ex.getMessage() + "|@"
                                + (isHidden ? " @|red (hidden)|@" : "")
                );
            }
            LazyEdge lazyEdge = new LazyEdge(edge.getId());
            VertexiumScript.getContextEdges().put(edgeIndexString, lazyEdge);
            edgeIndex++;
        }

        return out.toString();
    }

    private Vertex getV() {
        return getGraph().getVertex(getId(), FetchHint.ALL_INCLUDING_HIDDEN, getTime(), getAuthorizations());
    }

    public String getId() {
        return vertexId;
    }

    public void delete() {
        getGraph().deleteVertex(getV(), getAuthorizations());
        getGraph().flush();
    }
}

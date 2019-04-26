package org.vertexium.cli.model;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.vertexium.util.IterableUtils.toList;

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
        writer.println("  @|bold hidden:|@ " + e.isHidden(getAuthorizations()));
        writer.println("  @|bold visibility:|@ " + e.getVisibility());
        writer.println("  @|bold label:|@ " + e.getLabel());
        writer.println("  @|bold timestamp:|@ " + VertexiumScript.timestampToString(e.getTimestamp()));

        writer.println("  @|bold hidden visibilities:|@");
        List<Visibility> hiddenVisibilities = toList(e.getHiddenVisibilities());
        if (hiddenVisibilities.size() == 0) {
            writer.println("    none");
        } else {
            for (Visibility hiddenVisibility : hiddenVisibilities) {
                writer.println("    " + hiddenVisibility.getVisibilityString());
            }
        }

        writer.println("  @|bold extended data table names:|@");
        VertexiumScript.getContextExtendedDataTables().clear();
        ImmutableSet<String> extendedDataTableNames = e.getExtendedDataTableNames();
        if (extendedDataTableNames.size() == 0) {
            writer.println("    none");
        } else {
            int tableIndex = 0;
            for (String extendedDataTableName : extendedDataTableNames) {
                String tableIndexString = "t" + tableIndex;
                writer.println("    @|bold " + tableIndexString + ":|@ " + extendedDataTableName);
                LazyExtendedDataTable lazyExtendedDataTable = new LazyExtendedDataTable(ElementType.EDGE, e.getId(), extendedDataTableName);
                VertexiumScript.getContextExtendedDataTables().put(tableIndexString, lazyExtendedDataTable);
                tableIndex++;
            }
        }

        writer.println("  @|bold properties:|@");
        VertexiumScript.getContextProperties().clear();
        int propIndex = 0;
        for (Property prop : e.getProperties()) {
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
        return getGraph().getEdge(getId(), FetchHints.ALL_INCLUDING_HIDDEN, getTime(), getAuthorizations());
    }

    public String getId() {
        return edgeId;
    }

    public void delete() {
        getGraph().deleteEdge(getE(), getAuthorizations());
        getGraph().flush();
    }
}

package org.vertexium.cli.model;

import org.vertexium.*;
import org.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LazyExtendedDataTable extends ModelBase {
    private final ElementType elementType;
    private final String elementId;
    private final String tableName;

    public LazyExtendedDataTable(ElementType elementType, String elementId, String tableName) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);

        writer.println("");
        for (ExtendedDataRow row : getRows()) {
            writer.println("@|bold " + row.getId().getRowId() + ":|@");
            for (Property property : row.getProperties()) {
                Object value = property.getValue();
                writer.println("    @|bold " + property.getName() + ":"
                    + (property.getKey() == null ? "" : property.getKey()) + "|@ "
                    + VertexiumScript.valueToString(value, true));
            }
        }

        return out.toString();
    }

    public Iterable<ExtendedDataRow> getRows() {
        return getElement().getExtendedData(tableName);
    }

    public Element getElement() {
        switch (elementType) {
            case VERTEX:
                return getGraph().getVertex(getElementId(), getGraph().getDefaultFetchHints(), getTime(), getAuthorizations());
            case EDGE:
                return getGraph().getEdge(getElementId(), getGraph().getDefaultFetchHints(), getTime(), getAuthorizations());
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    public String getElementId() {
        return elementId;
    }
}

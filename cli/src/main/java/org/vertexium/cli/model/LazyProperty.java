package org.vertexium.cli.model;

import org.vertexium.*;
import org.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;

import static org.vertexium.util.IterableUtils.toList;

public abstract class LazyProperty extends ModelBase {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;

    public LazyProperty(String propertyKey, String propertyName, Visibility propertyVisibility) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    @Override
    public String toString() {
        Property prop = getP();
        if (prop == null) {
            return null;
        }

        return toString(prop, getToStringHeaderLine());
    }

    public static String toString(Property prop, String headerLine) {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);
        if (headerLine != null) {
            writer.println(headerLine);
        }
        writer.println("  @|bold key:|@ " + prop.getKey());
        writer.println("  @|bold name:|@ " + prop.getName());
        writer.println("  @|bold visibility:|@ " + prop.getVisibility());
        writer.println("  @|bold timestamp:|@ " + VertexiumScript.timestampToString(prop.getTimestamp()));

        writer.println("  @|bold metadata:|@");
        Collection<Metadata.Entry> metadataEntries = prop.getMetadata().entrySet();
        if (metadataEntries.size() == 0) {
            writer.println("    none");
        } else {
            for (Metadata.Entry m : metadataEntries) {
                writer.println("    " + m.getKey() + "[" + m.getVisibility() + "]: " + VertexiumScript.valueToString(m.getValue(), false));
            }
        }

        writer.println("  @|bold hidden visibilities:|@");
        List<Visibility> hiddenVisibilities = toList(prop.getHiddenVisibilities());
        if (hiddenVisibilities.size() == 0) {
            writer.println("    none");
        } else {
            for (Visibility hiddenVisibility : hiddenVisibilities) {
                writer.println("    " + hiddenVisibility.getVisibilityString());
            }
        }

        writer.println("  @|bold value:|@" + VertexiumScript.valueToString(prop.getValue(), true));

        return out.toString();
    }

    protected abstract String getToStringHeaderLine();

    protected abstract Element getE();

    protected abstract Property getP();

    public String getKey() {
        return propertyKey;
    }

    public String getName() {
        return propertyName;
    }

    public Visibility getVisibility() {
        return propertyVisibility;
    }

    public void delete() {
        getE().deleteProperty(getKey(), getName(), getVisibility(), getAuthorizations());
        getGraph().flush();
    }
}

package org.neolumin.vertexium.cli.model;

import org.neolumin.vertexium.Metadata;
import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.Visibility;
import org.neolumin.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;

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

        writer.println("  @|bold metadata:|@");
        for (Metadata.Entry m : prop.getMetadata().entrySet()) {
            writer.println("    " + m.getKey() + "[" + m.getVisibility() + "]: " + VertexiumScript.valueToString(m.getValue(), false));
        }

        writer.println("  @|bold value:|@");
        writer.println(VertexiumScript.valueToString(prop.getValue(), true));

        return out.toString();
    }

    protected abstract String getToStringHeaderLine();

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
}

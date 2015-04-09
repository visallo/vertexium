package org.vertexium.cli.model;

import org.vertexium.*;
import org.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

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
        writer.println("  @|bold timestamp:|@ " + prop.getTimestamp());

        writer.println("  @|bold metadata:|@");
        for (Metadata.Entry m : prop.getMetadata().entrySet()) {
            writer.println("    " + m.getKey() + "[" + m.getVisibility() + "]: " + VertexiumScript.valueToString(m.getValue(), false));
        }

        writer.println("  @|bold value:|@");
        writer.println(VertexiumScript.valueToString(prop.getValue(), true));

        return out.toString();
    }

    public String getHistory() {
        Element e = getE();
        if (e == null) {
            return null;
        }
        Iterable<HistoricalPropertyValue> historicalValues = e.getHistoricalPropertyValues(getKey(), getName(), getVisibility(), getAuthorizations());

        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);
        writer.println("@|bold history:|@");
        for (HistoricalPropertyValue historicalValue : historicalValues) {
            writer.println("  @|bold " + new Date(historicalValue.getTimestamp()) + " (" + historicalValue.getTimestamp() + "):|@");
            writer.println("    @|bold value:|@");
            writer.println(VertexiumScript.valueToString(historicalValue.getValue(), true));
            writer.println("    @|bold metadata:|@");
            for (Metadata.Entry m : historicalValue.getMetadata().entrySet()) {
                writer.println("      " + m.getKey() + "[" + m.getVisibility() + "]: " + VertexiumScript.valueToString(m.getValue(), false));
            }
        }
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
}

package org.vertexium;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefinePropertyBuilder {
    private final String propertyName;
    protected Class dataType = String.class;
    protected Set<TextIndexHint> textIndexHints = new HashSet<TextIndexHint>();
    private Double boost;

    DefinePropertyBuilder(String propertyName) {
        this.propertyName = propertyName;
    }

    public DefinePropertyBuilder dataType(Class dataType) {
        this.dataType = dataType;
        return this;
    }

    public DefinePropertyBuilder textIndexHint(Collection<TextIndexHint> textIndexHints) {
        this.textIndexHints.addAll(textIndexHints);
        return this;
    }

    public DefinePropertyBuilder textIndexHint(TextIndexHint... textIndexHints) {
        Collections.addAll(this.textIndexHints, textIndexHints);
        return this;
    }

    public PropertyDefinition define() {
        return new PropertyDefinition(
                propertyName,
                dataType,
                textIndexHints,
                boost
        );
    }

    public DefinePropertyBuilder boost(double boost) {
        this.boost = boost;
        return this;
    }
}

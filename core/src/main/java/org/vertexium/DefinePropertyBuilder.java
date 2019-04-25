package org.vertexium;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefinePropertyBuilder {
    private final String propertyName;
    protected Class dataType = String.class;
    protected Set<TextIndexHint> textIndexHints = new HashSet<>();
    private Double boost;
    private boolean sortable;

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
            boost,
            sortable
        );
    }

    public DefinePropertyBuilder boost(double boost) {
        this.boost = boost;
        return this;
    }

    public DefinePropertyBuilder sortable(boolean sortable) {
        this.sortable = sortable;
        return this;
    }
}

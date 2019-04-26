package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherRemoveLabelItem extends CypherRemoveItem {
    private final CypherVariable variable;
    private final CypherListLiteral<CypherLabelName> labelNames;

    public CypherRemoveLabelItem(CypherVariable variable, CypherListLiteral<CypherLabelName> labelNames) {
        this.variable = variable;
        this.labelNames = labelNames;
    }

    public CypherListLiteral<CypherLabelName> getLabelNames() {
        return labelNames;
    }

    public CypherVariable getVariable() {
        return variable;
    }

    @Override
    public String toString() {
        return String.format("%s%s", getVariable(), getLabelNames());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.concat(
            Stream.of(variable),
            labelNames.stream()
        );
    }
}

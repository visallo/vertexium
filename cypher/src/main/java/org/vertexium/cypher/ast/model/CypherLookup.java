package org.vertexium.cypher.ast.model;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherLookup extends CypherAstBase {
    private final CypherAstBase atom;
    private final String property;
    private final List<CypherLabelName> labels;

    public CypherLookup(CypherAstBase atom, String property, List<CypherLabelName> labels) {
        this.atom = atom;
        this.property = property;
        this.labels = labels;
    }

    public CypherAstBase getAtom() {
        return atom;
    }

    public String getProperty() {
        return property;
    }

    public List<CypherLabelName> getLabels() {
        return labels;
    }

    public boolean hasLabels() {
        return getLabels() != null && getLabels().size() > 0;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(getAtom());
        if (hasLabels()) {
            result.append(getLabels().stream().map(CypherLabelName::toString).collect(Collectors.joining("")));
        }
        if (getProperty() != null) {
            result.append(".");
            result.append(getProperty());
        }
        return result.toString();
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.concat(
                Stream.of(atom),
                labels.stream().flatMap(CypherLiteral::getChildren)
        );
    }
}

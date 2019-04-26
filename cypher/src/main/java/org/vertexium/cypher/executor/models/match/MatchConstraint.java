package org.vertexium.cypher.executor.models.match;

import org.vertexium.cypher.ast.model.CypherElementPattern;
import org.vertexium.cypher.ast.model.CypherRangeLiteral;
import org.vertexium.cypher.ast.model.CypherRelationshipPattern;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class MatchConstraint<TCypherElementPattern extends CypherElementPattern, TConnected extends MatchConstraint> {
    private final String name;
    private final String hashName;
    private final List<TConnected> connectedConstraints = new ArrayList<>();
    private final List<TCypherElementPattern> patterns;
    private boolean optional;

    protected MatchConstraint(String name, List<TCypherElementPattern> patterns, boolean optional) {
        this.name = name;
        if (name == null) {
            hashName = UUID.randomUUID().toString();
        } else {
            hashName = name;
        }
        this.patterns = patterns;
        this.optional = optional;
    }

    public String getName() {
        return name;
    }

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    public List<TConnected> getConnectedConstraints() {
        return connectedConstraints;
    }

    public void addConnectedConstraint(TConnected constraint) {
        checkNotNull(constraint, "connected constraint cannot be null");
        this.connectedConstraints.add(constraint);
    }

    public List<TCypherElementPattern> getPatterns() {
        return patterns;
    }

    @SuppressWarnings("unchecked")
    public static <TCypherElementPattern extends CypherElementPattern, TConnected extends MatchConstraint> void merge(
        MatchConstraint<TCypherElementPattern, TConnected> src,
        MatchConstraint<TCypherElementPattern, TConnected> dest
    ) {
        dest.connectedConstraints.addAll(src.getConnectedConstraints());
        for (TConnected connectedConstraint : dest.connectedConstraints) {
            if (connectedConstraint.getConnectedConstraints().contains(src)) {
                connectedConstraint.getConnectedConstraints().remove(src);
                connectedConstraint.getConnectedConstraints().add(dest);
            }
        }
        dest.patterns.addAll(src.patterns);
        dest.setOptional(src.isOptional() && dest.isOptional());
    }

    @Override
    public String toString() {
        return "MatchConstraint{" +
            "name='" + name + '\'' +
            ", hashName='" + hashName + '\'' +
            ", patterns=" + patterns +
            ", optional=" + optional +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MatchConstraint that = (MatchConstraint) o;

        if (!hashName.equals(that.hashName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return hashName.hashCode();
    }

    public boolean hasZeroRangePattern() {
        for (CypherElementPattern elementPattern : getPatterns()) {
            if (elementPattern instanceof CypherRelationshipPattern) {
                CypherRangeLiteral range = ((CypherRelationshipPattern) elementPattern).getRange();
                if (range != null) {
                    Integer from = range.getFrom();
                    if (from != null && from == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public int getConstraintCount() {
        return getPatterns().stream()
            .map(CypherElementPattern::getConstraintCount)
            .reduce(0, (i1, i2) -> i1 + i2);
    }
}

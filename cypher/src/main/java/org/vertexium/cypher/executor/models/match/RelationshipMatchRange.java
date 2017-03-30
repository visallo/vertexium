package org.vertexium.cypher.executor.models.match;

import org.vertexium.cypher.ast.model.CypherRangeLiteral;
import org.vertexium.cypher.ast.model.CypherRelationshipPattern;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

public class RelationshipMatchRange {
    private final boolean rangeSet;
    private final Integer from;
    private final Integer to;

    public RelationshipMatchRange(CypherRelationshipPattern relationshipPattern) {
        CypherRangeLiteral range = relationshipPattern.getRange();
        rangeSet = range != null;
        if (rangeSet) {
            from = range.getFrom() == null ? 1 : range.getFrom();
            to = range.getTo();
        } else {
            from = null;
            to = null;
        }
    }

    public RelationshipMatchRange merge(CypherRelationshipPattern relationshipPattern) {
        RelationshipMatchRange other = new RelationshipMatchRange(relationshipPattern);
        if (equals(other)) {
            return other;
        } else {
            throw new VertexiumCypherNotImplemented("Cannot merge differing range set " + this + " != " + other);
        }
    }

    public boolean isIn(int i) {
        return !isBefore(i) && !isAfter(i);
    }

    public boolean isRangeSet() {
        return rangeSet;
    }

    public boolean isBefore(int i) {
        return from != null && i < from;
    }

    public boolean isAfter(int i) {
        return to != null && i > to;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getTo() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RelationshipMatchRange that = (RelationshipMatchRange) o;

        if (rangeSet != that.rangeSet) {
            return false;
        }
        if (from != null ? !from.equals(that.from) : that.from != null) {
            return false;
        }
        return to != null ? to.equals(that.to) : that.to == null;
    }

    @Override
    public int hashCode() {
        int result = (rangeSet ? 1 : 0);
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (to != null ? to.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RelationshipMatchRange{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }
}

package org.vertexium.cypher.ast.model;

public class CypherRelationshipPattern extends CypherElementPattern {
    private final CypherListLiteral<CypherRelTypeName> relTypeNames;
    private final CypherRangeLiteral range;
    private CypherDirection direction;
    private CypherNodePattern previousNodePattern;
    private CypherNodePattern nextNodePattern;

    public CypherRelationshipPattern(
        String name,
        CypherListLiteral<CypherRelTypeName> relTypeNames,
        CypherMapLiteral<String, CypherAstBase> properties,
        CypherRangeLiteral range,
        CypherDirection direction
    ) {
        super(name, properties);
        this.relTypeNames = relTypeNames;
        this.range = range;
        this.direction = direction;
    }

    public CypherDirection getDirection() {
        return direction;
    }

    public CypherRangeLiteral getRange() {
        return range;
    }

    public CypherListLiteral<CypherRelTypeName> getRelTypeNames() {
        return relTypeNames;
    }

    public boolean hasRelTypeNames() {
        return getRelTypeNames() != null && getRelTypeNames().size() > 0;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (getDirection().hasIn()) {
            result.append("<");
        }
        result.append("-");
        if (getName() != null
            || (getPropertiesMap() != null && getPropertiesMap().size() > 0)
            || (getRelTypeNames() != null && getRelTypeNames().size() > 0)
            || getRange() != null) {
            result.append("[");
            if (getName() != null) {
                result.append(getName());
            }
            if (getRelTypeNames() != null && getRelTypeNames().size() > 0) {
                result.append(getRelTypeNames());
            }
            if (getPropertiesMap() != null && getPropertiesMap().size() > 0) {
                result.append(" ").append(getPropertiesMap());
            }
            if (getRange() != null) {
                result.append("*");
                result.append(getRange());
            }
            result.append("]");
        }
        result.append("-");
        if (getDirection().hasOut()) {
            result.append(">");
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        CypherRelationshipPattern that = (CypherRelationshipPattern) o;

        if (relTypeNames != null ? !relTypeNames.equals(that.relTypeNames) : that.relTypeNames != null) {
            return false;
        }
        if (range != null ? !range.equals(that.range) : that.range != null) {
            return false;
        }
        if (direction != that.direction) {
            return false;
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (relTypeNames != null ? relTypeNames.hashCode() : 0);
        result = 31 * result + (range != null ? range.hashCode() : 0);
        result = 31 * result + (direction != null ? direction.hashCode() : 0);
        return result;
    }

    @Override
    public int getConstraintCount() {
        return super.getConstraintCount() + (getRelTypeNames() == null ? 0 : getRelTypeNames().size());
    }

    public void setPreviousNodePattern(CypherNodePattern previousNodePattern) {
        this.previousNodePattern = previousNodePattern;
    }

    public CypherNodePattern getPreviousNodePattern() {
        return previousNodePattern;
    }

    public void setNextNodePattern(CypherNodePattern nextNodePattern) {
        this.nextNodePattern = nextNodePattern;
    }

    public CypherNodePattern getNextNodePattern() {
        return nextNodePattern;
    }
}

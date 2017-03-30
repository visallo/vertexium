package org.vertexium.cypher.ast.model;

import org.vertexium.Direction;
import org.vertexium.VertexiumException;

public enum CypherDirection {
    OUT, IN, UNSPECIFIED, BOTH;

    public boolean hasIn() {
        return this == IN;
    }

    public boolean hasOut() {
        return this == OUT;
    }

    public boolean isDirected() {
        return hasIn() || hasOut();
    }

    public Direction toVertexiumDirection() {
        switch (this) {
            case OUT:
                return Direction.OUT;
            case IN:
                return Direction.IN;
            case BOTH:
            case UNSPECIFIED:
                return Direction.BOTH;
            default:
                throw new VertexiumException("unexpected direction: " + this);
        }
    }

    public CypherDirection merge(CypherDirection direction) {
        switch (this) {
            case OUT:
                if (direction == IN || direction == BOTH) {
                    return BOTH;
                }
                return this;
            case IN:
                if (direction == OUT || direction == BOTH) {
                    return BOTH;
                }
                return this;
            case BOTH:
                return BOTH;
            case UNSPECIFIED:
                return direction;
            default:
                throw new VertexiumException("unexpected direction: " + this);
        }
    }
}

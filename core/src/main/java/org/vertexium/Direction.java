package org.vertexium;

public enum Direction {
    OUT,
    IN,
    BOTH;

    public Direction reverse() {
        switch (this) {
            case OUT:
                return IN;
            case IN:
                return OUT;
            case BOTH:
                return BOTH;
            default:
                throw new VertexiumException("unexpected direction: " + this);
        }
    }
}

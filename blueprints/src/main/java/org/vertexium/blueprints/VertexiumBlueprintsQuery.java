package org.vertexium.blueprints;

import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Query;
import org.vertexium.VertexiumException;

public abstract class VertexiumBlueprintsQuery implements Query {
    protected org.vertexium.query.Predicate toVertexiumPredicate(Predicate predicate) {
        if (predicate.equals(com.tinkerpop.blueprints.Compare.EQUAL)) {
            return org.vertexium.query.Compare.EQUAL;
        }
        if (predicate.equals(com.tinkerpop.blueprints.Compare.NOT_EQUAL)) {
            return org.vertexium.query.Compare.NOT_EQUAL;
        }
        if (predicate.equals(com.tinkerpop.blueprints.Compare.GREATER_THAN)) {
            return org.vertexium.query.Compare.GREATER_THAN;
        }
        if (predicate.equals(com.tinkerpop.blueprints.Compare.GREATER_THAN_EQUAL)) {
            return org.vertexium.query.Compare.GREATER_THAN_EQUAL;
        }
        if (predicate.equals(com.tinkerpop.blueprints.Compare.LESS_THAN)) {
            return org.vertexium.query.Compare.LESS_THAN;
        }
        if (predicate.equals(com.tinkerpop.blueprints.Compare.LESS_THAN_EQUAL)) {
            return org.vertexium.query.Compare.LESS_THAN_EQUAL;
        }

        if (predicate.equals(com.tinkerpop.blueprints.Contains.IN)) {
            return org.vertexium.query.Contains.IN;
        }
        if (predicate.equals(com.tinkerpop.blueprints.Contains.NOT_IN)) {
            return org.vertexium.query.Contains.NOT_IN;
        }

        throw new VertexiumException("Could not convert Blueprints predicate '" + predicate + "' to Vertexium predicate");
    }
}

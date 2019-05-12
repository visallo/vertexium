package org.vertexium.elasticsearch5;

import org.vertexium.Authorizations;
import org.vertexium.VertexiumException;
import org.vertexium.Visibility;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

public class Elasticsearch5GraphAuthorizations implements Authorizations {
    private static final long serialVersionUID = -270047775774669396L;
    private final String[] auths;

    public Elasticsearch5GraphAuthorizations(String[] auths) {
        this.auths = auths;
    }

    @Override
    public boolean canRead(Visibility visibility) {
        checkNotNull(visibility, "visibility is required");

        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        if (visibility.getVisibilityString().length() == 0) {
            return true;
        }

        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new org.vertexium.security.Authorizations(this.getAuthorizations()));
        ColumnVisibility columnVisibility = new ColumnVisibility(visibility.getVisibilityString());
        try {
            return visibilityEvaluator.evaluate(columnVisibility);
        } catch (VisibilityParseException e) {
            throw new VertexiumException("could not evaluate visibility " + visibility.getVisibilityString(), e);
        }
    }

    @Override
    public String[] getAuthorizations() {
        return auths;
    }

    @Override
    public boolean equals(Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public String toString() {
        return "Elasticsearch5GraphAuthorizations{" +
            "auths=" + Arrays.toString(auths) +
            '}';
    }
}

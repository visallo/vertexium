package org.vertexium.inmemory;

import org.vertexium.VertexiumException;
import org.vertexium.Visibility;
import org.vertexium.security.Authorizations;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.util.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

public class InMemoryAuthorizations implements org.vertexium.Authorizations, Serializable {
    private static final long serialVersionUID = 1L;
    private final String[] authorizations;

    public InMemoryAuthorizations(String... authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    public String[] getAuthorizations() {
        return authorizations;
    }

    @Override
    public boolean equals(org.vertexium.Authorizations authorizations) {
        return ArrayUtils.intersectsAll(getAuthorizations(), authorizations.getAuthorizations());
    }

    @Override
    public String toString() {
        return Arrays.toString(authorizations);
    }

    @Override
    public boolean canRead(Visibility visibility) {
        checkNotNull(visibility, "visibility is required");

        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        if (visibility.getVisibilityString().length() == 0) {
            return true;
        }

        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new Authorizations(this.getAuthorizations()));
        ColumnVisibility columnVisibility = new ColumnVisibility(visibility.getVisibilityString());
        try {
            return visibilityEvaluator.evaluate(columnVisibility);
        } catch (VisibilityParseException e) {
            throw new VertexiumException("could not evaluate visibility " + visibility.getVisibilityString(), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InMemoryAuthorizations that = (InMemoryAuthorizations) o;

        return ArrayUtils.intersectsAll(getAuthorizations(), that.getAuthorizations());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(authorizations);
    }
}

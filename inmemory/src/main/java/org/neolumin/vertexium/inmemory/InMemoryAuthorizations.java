package org.neolumin.vertexium.inmemory;

import org.neolumin.vertexium.VertexiumException;
import org.neolumin.vertexium.Visibility;
import org.neolumin.vertexium.inmemory.security.Authorizations;
import org.neolumin.vertexium.inmemory.security.ColumnVisibility;
import org.neolumin.vertexium.inmemory.security.VisibilityEvaluator;
import org.neolumin.vertexium.inmemory.security.VisibilityParseException;
import org.neolumin.vertexium.util.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;

import static org.neolumin.vertexium.util.Preconditions.checkNotNull;

public class InMemoryAuthorizations implements org.neolumin.vertexium.Authorizations, Serializable {
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
    public boolean equals(org.neolumin.vertexium.Authorizations authorizations) {
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
}

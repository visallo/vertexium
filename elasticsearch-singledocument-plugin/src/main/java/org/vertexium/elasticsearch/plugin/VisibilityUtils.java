package org.vertexium.elasticsearch.plugin;

import org.vertexium.security.Authorizations;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;

public class VisibilityUtils {
    public static boolean canRead(String visibility, String[] authorizations) {
        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new Authorizations(authorizations));
        ColumnVisibility columnVisibility = new ColumnVisibility(visibility);
        try {
            return visibilityEvaluator.evaluate(columnVisibility);
        } catch (VisibilityParseException ex) {
            throw new RuntimeException("could not evaluate visibility " + visibility, ex);
        }
    }
}

package org.vertexium.elasticsearch5.plugin.utils;

import org.vertexium.elasticsearch5.plugin.VertexiumElasticsearchPluginException;
import org.vertexium.security.Authorizations;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;

import static org.vertexium.elasticsearch5.plugin.utils.Preconditions.checkNotNull;

public class VisibilityUtils {
    public static boolean canRead(String visibility, String[] authorizations) {
        checkNotNull(visibility, "visibility is required");
        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new Authorizations(authorizations));
        ColumnVisibility columnVisibility = new ColumnVisibility(visibility);
        try {
            return visibilityEvaluator.evaluate(columnVisibility);
        } catch (VisibilityParseException ex) {
            throw new VertexiumElasticsearchPluginException("could not evaluate visibility " + visibility, ex);
        }
    }
}

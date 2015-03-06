package org.neolumin.vertexium.accumulo.blueprints;

import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.accumulo.AccumuloAuthorizations;
import org.neolumin.vertexium.blueprints.AuthorizationsProvider;

import java.util.Map;

public class AccumuloAuthorizationsProvider implements AuthorizationsProvider {
    private String[] authorizations;

    public AccumuloAuthorizationsProvider(Map configuration) {
        String authString = (String) configuration.get("authorizationsProvider.auths");
        if (authString == null || authString.length() == 0) {
            this.authorizations = new String[0];
        } else {
            this.authorizations = authString.split(",");
        }
    }

    @Override
    public Authorizations getAuthorizations() {
        return new AccumuloAuthorizations(authorizations);
    }
}

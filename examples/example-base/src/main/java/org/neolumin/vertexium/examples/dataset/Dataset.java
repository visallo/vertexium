package org.neolumin.vertexium.examples.dataset;

import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;

import java.io.IOException;

public abstract class Dataset {
    public abstract void load(Graph graph, int numberOfVerticesToCreate, String[] visibilities, Authorizations authorizations) throws IOException;
}

package org.vertexium.examples.dataset;

import org.vertexium.Authorizations;
import org.vertexium.Graph;

import java.io.IOException;

public abstract class Dataset {
    public abstract void load(Graph graph, int numberOfVerticesToCreate, String[] visibilities, Authorizations authorizations) throws IOException;
}

package org.vertexium.accumulo;

import org.vertexium.Metadata;
import org.vertexium.VertexiumSerializer;
import org.vertexium.id.NameSubstitutionStrategy;

public abstract class LazyPropertyMetadata {
    public abstract Metadata toMetadata(
            VertexiumSerializer vertexiumSerializer,
            NameSubstitutionStrategy nameSubstitutionStrategy
    );
}

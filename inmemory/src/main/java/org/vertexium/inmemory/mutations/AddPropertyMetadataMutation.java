package org.vertexium.inmemory.mutations;

import org.vertexium.FetchHints;
import org.vertexium.Metadata;
import org.vertexium.Visibility;

public class AddPropertyMetadataMutation extends PropertyMutation {
    private final Metadata metadata;

    public AddPropertyMetadataMutation(
        long timestamp, String key, String name, Metadata metadata, Visibility visibility) {
        super(timestamp, key, name, visibility, visibility);
        this.metadata = Metadata.create(metadata);
    }

    public Metadata getMetadata(FetchHints fetchHints) {
        return Metadata.create(metadata, fetchHints);
    }
}

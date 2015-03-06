package org.neolumin.vertexium.id;

import org.neolumin.vertexium.GraphConfiguration;

import java.util.UUID;

public class UUIDIdGenerator implements IdGenerator {
    public UUIDIdGenerator(GraphConfiguration configuration) {

    }

    @Override
    public String nextId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}

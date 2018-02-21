package org.vertexium.id;

import org.apache.commons.lang3.StringUtils;
import org.vertexium.GraphConfiguration;

import java.util.UUID;

public class UUIDIdGenerator implements IdGenerator {
    public UUIDIdGenerator(GraphConfiguration configuration) {

    }

    @Override
    public String nextId() {
        return StringUtils.remove(UUID.randomUUID().toString(), '-');
    }
}

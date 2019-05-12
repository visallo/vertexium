package org.vertexium.elasticsearch5.models;

import org.elasticsearch.search.SearchHitField;
import org.vertexium.FetchHints;
import org.vertexium.User;
import org.vertexium.Visibility;
import org.vertexium.elasticsearch5.ScriptService;
import org.vertexium.elasticsearch5.StreamingPropertyValueService;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LazyProperties implements Iterable<org.vertexium.Property> {
    private final SearchHitField field;
    private final FetchHints fetchHints;
    private final StreamingPropertyValueService streamingPropertyValueService;
    private final ScriptService scriptService;
    private final User user;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private List<org.vertexium.Property> properties;

    public LazyProperties(
        SearchHitField field,
        FetchHints fetchHints,
        StreamingPropertyValueService streamingPropertyValueService,
        ScriptService scriptService,
        User user
    ) {
        this.field = field;
        this.fetchHints = fetchHints;
        this.streamingPropertyValueService = streamingPropertyValueService;
        this.scriptService = scriptService;
        this.user = user;
    }

    @Override
    public Iterator<org.vertexium.Property> iterator() {
        return getProperties().iterator();
    }

    private List<org.vertexium.Property> getProperties() {
        lock.readLock().lock();
        try {
            if (properties != null) {
                return properties;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            if (properties != null) {
                return properties;
            }
            if (field == null) {
                properties = Collections.emptyList();
                return null;
            }
            Properties props = ProtobufUtils.propertiesFromField(field.getValue());
            properties = new ArrayList<>();
            for (Property prop : props.getPropertiesList()) {
                if (prop.getSoftDelete()) {
                    continue;
                }
                if (!fetchHints.isIncludeHidden() && isHidden(prop, user)) {
                    continue;
                }
                org.vertexium.Property p = scriptService.protobufPropertyToVertexium(prop, fetchHints);
                if (!user.canRead(p.getVisibility())) {
                    continue;
                }
                properties.add(p);
            }
            return properties;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean isHidden(Property prop, User user) {
        if (prop.getHiddenVisibilitiesList() == null) {
            return false;
        }
        for (PropertyHiddenVisibility hiddenVisibility : prop.getHiddenVisibilitiesList()) {
            if (user.canRead(new Visibility(hiddenVisibility.getVisibility()))) {
                return true;
            }
        }
        return false;
    }
}

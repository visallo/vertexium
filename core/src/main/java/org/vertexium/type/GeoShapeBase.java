package org.vertexium.type;

import org.vertexium.util.GeoUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class GeoShapeBase implements GeoShape, Serializable {
    private static final long serialVersionUID = 6993185229233913152L;

    private final String description;

    public GeoShapeBase() {
        description = null;
    }

    public GeoShapeBase(String description) {
        this.description = description;
    }

    @Override
    public void validate() {
    }

    @Override
    public boolean intersects(GeoShape geoShape) {
        return GeoUtils.intersects(this, geoShape);
    }

    @Override
    public boolean within(GeoShape geoShape) {
        return GeoUtils.within(this, geoShape);
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * This is used to fix Kryo serialization issues with lists generated from methods such as
     * {@link java.util.Arrays#asList(Object[])}
     */
    protected <T> List<? extends List<T>> toArrayLists(List<List<T>> lists) {
        if (lists == null) {
            return new ArrayList<>();
        }

        for (int i = 0; i < lists.size(); i++) {
            List<T> list = lists.get(i);
            lists.set(i, toArrayList(list));
        }
        return lists;
    }

    /**
     * This is used to fix Kryo serialization issues with lists generated from methods such as
     * {@link java.util.Arrays#asList(Object[])}
     */
    protected <T> List<T> toArrayList(List<T> list) {
        if (list == null) {
            return null;
        }
        if (!list.getClass().equals(ArrayList.class)) {
            list = new ArrayList<>(list);
        }
        return list;
    }
}

package org.vertexium.util;

import org.vertexium.Element;

public class ToElementIterable<T extends Element> extends ConvertingIterable<T, Element> {
    public ToElementIterable(Iterable<T> iterable) {
        super(iterable);
    }

    @Override
    protected Element convert(T o) {
        return o;
    }
}

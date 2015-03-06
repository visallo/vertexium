package org.neolumin.vertexium.util;

import java.io.Closeable;

public interface CloseableIterable<T> extends Iterable<T>, Closeable {
}

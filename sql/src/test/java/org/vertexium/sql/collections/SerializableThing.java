package org.vertexium.sql.collections;

import com.google.common.base.Objects;

import java.io.Serializable;

class SerializableThing implements Serializable {
    public final int i;
    public final String s;

    public SerializableThing(int i) {
        this.i = i;
        this.s = "value" + i;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SerializableThing that = (SerializableThing) o;
        return Objects.equal(i, that.i) &&
                Objects.equal(s, that.s);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(i, s);
    }
}

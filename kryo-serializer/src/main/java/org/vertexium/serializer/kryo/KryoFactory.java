package org.vertexium.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import org.vertexium.VertexiumException;
import org.vertexium.type.GeoCircle;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

import java.util.Date;
import java.util.HashMap;

public class KryoFactory {
    public Kryo createKryo() {
        Kryo kryo = new Kryo(new DefaultClassResolver(), new MapReferenceResolver() {
            @Override
            public boolean useReferences(Class type) {
                // avoid calling System.identityHashCode
                if (type == String.class || type == Date.class) {
                    return false;
                }
                return super.useReferences(type);
            }
        });
        registerClasses(kryo);

        kryo.setAutoReset(true);
        return kryo;
    }

    private void registerClasses(Kryo kryo) {
        kryo.register(GeoPoint.class, 1001);
        kryo.register(HashMap.class, 1002);
        kryo.register(GeoRect.class, 1006);
        kryo.register(GeoCircle.class, 1007);
        kryo.register(Date.class, 1008);
        registerAccumuloClasses(kryo);
    }

    private void registerAccumuloClasses(Kryo kryo) {
        try {
            Class.forName("org.vertexium.accumulo.AccumuloGraph");
        } catch (ClassNotFoundException e) {
            // this is ok and expected if Accumulo is not in the classpath
            return;
        }
        try {
            kryo.register(Class.forName("org.vertexium.accumulo.iterator.model.EdgeInfo"), 1000);
            kryo.register(Class.forName("org.vertexium.accumulo.StreamingPropertyValueRef"), 1003);
            kryo.register(Class.forName("org.vertexium.accumulo.StreamingPropertyValueTableRef"), 1004);
            kryo.register(Class.forName("org.vertexium.accumulo.StreamingPropertyValueHdfsRef"), 1005);
        } catch (ClassNotFoundException ex) {
            throw new VertexiumException("Could not find accumulo classes to serialize", ex);
        }
    }
}

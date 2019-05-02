package org.vertexium.accumulo.util;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.vertexium.VertexiumException;

import java.lang.reflect.Field;

public class ConnectorUtils {
    public static ClientContext getContext(Connector connector) {
        if (!(connector instanceof ConnectorImpl)) {
            throw new VertexiumException("Invalid connector type. Expected " + ConnectorImpl.class.getName() + " found " + connector.getClass().getName());
        }
        try {
            Field f = ConnectorImpl.class.getDeclaredField("context");
            f.setAccessible(true);
            return (ClientContext) f.get(connector);
        } catch (Exception e) {
            throw new VertexiumException("Could not get context field", e);
        }
    }
}

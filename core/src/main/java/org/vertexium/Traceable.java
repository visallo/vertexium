package org.vertexium;

import java.util.Map;

public interface Traceable {
    void traceOn(String description);

    void traceOn(String description, Map<String, String> data);

    void traceOff();
}

package org.vertexium.examples;

import com.v5analytics.webster.Handler;

import javax.servlet.http.HttpServletRequest;

public abstract class HandlerBase implements Handler {
    protected static String getRequiredParameter(HttpServletRequest request, String name) {
        try {
            String val = request.getParameter(name);
            if (val == null) {
                throw new RuntimeException("Parameter " + name + " is required");
            }
            return val;
        } catch (Exception ex) {
            throw new RuntimeException("Could not get parameter: " + name, ex);
        }
    }
}

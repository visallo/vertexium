package org.vertexium.cli.model;

import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.cli.VertexiumScript;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.TreeSet;

public abstract class ModelBase {
    public Graph getGraph() {
        return VertexiumScript.getGraph();
    }

    public static Authorizations getAuthorizations() {
        return VertexiumScript.getAuthorizations();
    }

    public static Long getTime() {
        return VertexiumScript.getTime();
    }

    public String[] getMethods() {
        Set<String> methodsNames = new TreeSet<>();
        for (Method method : this.getClass().getMethods()) {
            methodsNames.add(method.getName());
        }
        for (Method method : Object.class.getMethods()) {
            methodsNames.remove(method.getName());
        }
        return methodsNames.toArray(new String[methodsNames.size()]);
    }

    public String[] getProperties() {
        Set<String> propertyNames = new TreeSet<>();
        for (String methodName : getMethods()) {
            if (methodName.startsWith("get")) {
                methodName = methodName.substring("get".length());
                methodName = methodName.substring(0, 1).toLowerCase() + (methodName.length() > 1 ? methodName.substring(1) : "");
                propertyNames.add(methodName);
            }
        }
        return propertyNames.toArray(new String[propertyNames.size()]);
    }
}

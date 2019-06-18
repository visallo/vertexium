package org.vertexium.cypher.ast.model;

import java.util.Map;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public abstract class CypherElementPattern extends CypherAstBase {
    private String name;
    private final CypherMapLiteral<String, CypherAstBase> propertiesMap;

    public CypherElementPattern(String name, CypherMapLiteral<String, CypherAstBase> propertiesMap) {
        if (propertiesMap == null) {
            propertiesMap = new CypherMapLiteral<>();
        }
        this.name = name;
        this.propertiesMap = propertiesMap;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CypherMapLiteral<String, CypherAstBase> getPropertiesMap() {
        return propertiesMap;
    }

    public boolean hasProperties() {
        return propertiesMap != null && propertiesMap.size() > 0;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return stream(propertiesMap.entrySet()).map(Map.Entry::getValue);
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
            "name='" + name + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CypherElementPattern that = (CypherElementPattern) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return propertiesMap.equals(that.propertiesMap);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + propertiesMap.hashCode();
        return result;
    }

    public int getConstraintCount() {
        return propertiesMap.size();
    }
}

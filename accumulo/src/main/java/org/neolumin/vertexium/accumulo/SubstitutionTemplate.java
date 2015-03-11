package org.neolumin.vertexium.accumulo;

public interface SubstitutionTemplate {
    public String deflate(String value);
    public String inflate(String value);
}

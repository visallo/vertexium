package org.neolumin.vertexium.accumulo;

public class IdentitySubstitutionTemplate implements SubstitutionTemplate {
    @Override
    public String deflate(String value) {
        return value;
    }

    @Override
    public String inflate(String value) {
        return value;
    }
}

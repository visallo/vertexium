package org.vertexium.cypher.ast.model;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherRelationshipsPattern extends CypherAstBase {
    private final CypherNodePattern nodePattern;
    private final List<CypherElementPattern> patternElementChains;

    public CypherRelationshipsPattern(CypherNodePattern nodePattern, List<CypherElementPattern> patternElementChains) {
        this.nodePattern = nodePattern;
        this.patternElementChains = patternElementChains;
    }

    public CypherNodePattern getNodePattern() {
        return nodePattern;
    }

    public List<CypherElementPattern> getPatternElementChains() {
        return patternElementChains;
    }

    @Override
    public String toString() {
        return String.format(
                "%s%s",
                getNodePattern(),
                getPatternElementChains().stream().map(CypherElementPattern::toString).collect(Collectors.joining(""))
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.concat(
                Stream.of(nodePattern),
                patternElementChains.stream()
        );
    }
}

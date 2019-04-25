package org.vertexium.cypher.executor.models.match;

import com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.Collectors;

public class PatternPartMatchConstraint {
    private final Map<String, List<MatchConstraint>> namedPaths;
    private final LinkedHashSet<MatchConstraint> matchConstraints;

    public PatternPartMatchConstraint(
        Map<String, List<MatchConstraint>> namedMatchConstraints,
        LinkedHashSet<MatchConstraint> matchConstraints
    ) {
        this.namedPaths = namedMatchConstraints;
        this.matchConstraints = matchConstraints;
    }

    public PatternPartMatchConstraint(String pathName, LinkedHashSet<MatchConstraint> matchConstraints) {
        namedPaths = new HashMap<>();
        if (pathName != null) {
            namedPaths.put(pathName, Lists.newArrayList(matchConstraints));
        }
        this.matchConstraints = matchConstraints;
    }

    public Map<String, List<MatchConstraint>> getNamedPaths() {
        return namedPaths;
    }

    public LinkedHashSet<MatchConstraint> getMatchConstraints() {
        return matchConstraints;
    }

    public Set<String> getPartNames() {
        return getMatchConstraints().stream()
            .map(MatchConstraint::getName)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PatternPartMatchConstraint that = (PatternPartMatchConstraint) o;

        if (namedPaths != null ? !namedPaths.equals(that.namedPaths) : that.namedPaths != null) {
            return false;
        }
        return matchConstraints != null ? matchConstraints.equals(that.matchConstraints) : that.matchConstraints == null;
    }

    @Override
    public int hashCode() {
        int result = namedPaths != null ? namedPaths.hashCode() : 0;
        result = 31 * result + (matchConstraints != null ? matchConstraints.hashCode() : 0);
        return result;
    }
}

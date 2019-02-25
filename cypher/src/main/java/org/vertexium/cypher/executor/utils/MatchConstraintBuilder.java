package org.vertexium.cypher.executor.utils;

import com.google.common.collect.Lists;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.models.match.*;

import java.util.*;
import java.util.stream.Collectors;

public class MatchConstraintBuilder {
    public MatchConstraints getMatchConstraints(List<CypherMatchClause> matchClauses) {
        MatchConstraints matchConstraints = null;
        for (CypherMatchClause matchClaus : matchClauses) {
            MatchConstraints newMatchConstraints = matchClauseToConstraints(matchClaus);
            if (matchConstraints == null) {
                matchConstraints = newMatchConstraints;
            } else {
                matchConstraints = mergeMatchConstraints(matchConstraints, newMatchConstraints);
            }
        }

        LinkedHashSet<PatternPartMatchConstraint> collapseConstraints = collapseConstraints(
            new LinkedList<>(matchConstraints.getPatternPartMatchConstraints())
        );

        return new MatchConstraints(collapseConstraints, matchConstraints.getWhereExpressions());
    }

    private MatchConstraints mergeMatchConstraints(MatchConstraints matchConstraintsA, MatchConstraints matchConstraintsB) {
        LinkedHashSet<PatternPartMatchConstraint> mergedConstraints = new LinkedHashSet<>();
        for (PatternPartMatchConstraint patternPartMatchConstraintA : matchConstraintsA.getPatternPartMatchConstraints()) {
            for (PatternPartMatchConstraint patternPartMatchConstraintB : matchConstraintsB.getPatternPartMatchConstraints()) {
                if (patternPartMatchConstraintHasOverlap(patternPartMatchConstraintA, patternPartMatchConstraintB)) {
                    mergedConstraints.add(mergePatternPartMatchConstraint(patternPartMatchConstraintA, patternPartMatchConstraintB));
                } else {
                    mergedConstraints.add(patternPartMatchConstraintA);
                    mergedConstraints.add(patternPartMatchConstraintB);
                }
            }
        }

        ArrayList<CypherAstBase> whereExpressions = new ArrayList<>();
        whereExpressions.addAll(matchConstraintsA.getWhereExpressions());
        whereExpressions.addAll(matchConstraintsB.getWhereExpressions());

        return new MatchConstraints(mergedConstraints, whereExpressions);
    }

    private LinkedHashSet<PatternPartMatchConstraint> collapseConstraints(LinkedList<PatternPartMatchConstraint> constraints) {
        LinkedHashSet<PatternPartMatchConstraint> results = new LinkedHashSet<>();
        while (constraints.size() > 0) {
            PatternPartMatchConstraint constraint = constraints.removeFirst();
            Optional<PatternPartMatchConstraint> overlappingConstraint = constraints.stream()
                .filter(c -> patternPartMatchConstraintHasOverlap(constraint, c))
                .findFirst();
            if (overlappingConstraint.isPresent()) {
                constraints.remove(overlappingConstraint.get());
                constraints.add(mergePatternPartMatchConstraint(constraint, overlappingConstraint.get()));
            } else {
                results.add(constraint);
            }
        }
        return results;
    }

    private PatternPartMatchConstraint mergePatternPartMatchConstraint(
        PatternPartMatchConstraint patternPartMatchConstraintA,
        PatternPartMatchConstraint patternPartMatchConstraintB
    ) {
        LinkedHashSet<MatchConstraint> matchConstraints = new LinkedHashSet<>();
        matchConstraints.addAll(patternPartMatchConstraintB.getMatchConstraints());
        for (MatchConstraint matchConstraintA : patternPartMatchConstraintA.getMatchConstraints()) {
            boolean foundMatch = false;
            if (matchConstraintA.getName() != null) {
                for (MatchConstraint matchConstraintB : patternPartMatchConstraintB.getMatchConstraints()) {
                    if (matchConstraintA.getName().equals(matchConstraintB.getName())) {
                        MatchConstraint.merge(matchConstraintA, matchConstraintB);
                        foundMatch = true;
                    }
                }
            }
            if (!foundMatch) {
                matchConstraints.add(matchConstraintA);
            }
        }

        Map<String, List<MatchConstraint>> namedMatchConstraints = new HashMap<>();
        namedMatchConstraints.putAll(patternPartMatchConstraintA.getNamedPaths());
        namedMatchConstraints.putAll(patternPartMatchConstraintB.getNamedPaths());

        return new PatternPartMatchConstraint(namedMatchConstraints, matchConstraints);
    }

    private boolean patternPartMatchConstraintHasOverlap(
        PatternPartMatchConstraint patternPartMatchConstraint,
        PatternPartMatchConstraint newPatternPartMatchConstraint
    ) {
        Set<String> partNames = patternPartMatchConstraint.getPartNames();
        Set<String> newPartNames = newPatternPartMatchConstraint.getPartNames();
        for (String newPartName : newPartNames) {
            if (partNames.contains(newPartName)) {
                return true;
            }
        }
        return false;
    }

    private MatchConstraints matchClauseToConstraints(CypherMatchClause cypherMatchClause) {
        LinkedList<PatternPartMatchConstraint> ll = new LinkedList<>(
            cypherMatchClause.getPatternParts().stream()
                .map(pp -> patternPartToConstraints(pp, cypherMatchClause.isOptional()))
                .collect(Collectors.toList())
        );
        LinkedHashSet<PatternPartMatchConstraint> patternPartMatchConstraints = collapseConstraints(ll);
        ArrayList<CypherAstBase> whereExpressions = cypherMatchClause.getWhereExpression() == null
            ? Lists.newArrayList()
            : Lists.newArrayList(cypherMatchClause.getWhereExpression());
        return new MatchConstraints(patternPartMatchConstraints, whereExpressions);
    }

    public PatternPartMatchConstraint patternPartToConstraints(
        CypherPatternPart patternPart,
        boolean optional
    ) {
        String pathName = patternPart.getName();
        CypherListLiteral<CypherElementPattern> elementPatterns = patternPart.getElementPatterns();
        LinkedHashSet<MatchConstraint> allConstraints = new LinkedHashSet<>();

        NodeMatchConstraint firstMatchConstraint = null;
        MatchConstraint previousConstraint = null;
        for (CypherElementPattern elementPattern : elementPatterns) {
            if (elementPattern instanceof CypherNodePattern) {
                Optional<NodeMatchConstraint> existingNodeMatchConstraint = allConstraints.stream()
                    .filter(c -> c.getName() != null && c.getName().equals(elementPattern.getName()))
                    .map(c -> (NodeMatchConstraint) c)
                    .findFirst();
                NodeMatchConstraint nodeMatchConstraint = existingNodeMatchConstraint.orElseGet(
                    () -> new NodeMatchConstraint(
                        elementPattern.getName(),
                        Lists.newArrayList(),
                        optional
                    )
                );
                nodeMatchConstraint.getPatterns().add((CypherNodePattern) elementPattern);
                allConstraints.add(nodeMatchConstraint);
                if (firstMatchConstraint == null) {
                    firstMatchConstraint = nodeMatchConstraint;
                }
                if (previousConstraint != null) {
                    //noinspection RedundantCast
                    nodeMatchConstraint.addConnectedConstraint((RelationshipMatchConstraint) previousConstraint);
                    //noinspection RedundantCast
                    ((RelationshipMatchConstraint) previousConstraint).addConnectedConstraint(nodeMatchConstraint);
                }
                previousConstraint = nodeMatchConstraint;
            } else if (elementPattern instanceof CypherRelationshipPattern) {
                RelationshipMatchConstraint relationshipMatchConstraint = new RelationshipMatchConstraint(
                    elementPattern.getName(),
                    Lists.newArrayList((CypherRelationshipPattern) elementPattern),
                    optional
                );
                allConstraints.add(relationshipMatchConstraint);
                if (previousConstraint != null) {
                    //noinspection RedundantCast
                    relationshipMatchConstraint.addConnectedConstraint((NodeMatchConstraint) previousConstraint);
                    //noinspection RedundantCast
                    ((NodeMatchConstraint) previousConstraint).addConnectedConstraint(relationshipMatchConstraint);
                }
                previousConstraint = relationshipMatchConstraint;
            } else {
                throw new VertexiumCypherTypeErrorException(elementPattern, CypherNodePattern.class, CypherRelationshipPattern.class);
            }
        }

        return new PatternPartMatchConstraint(pathName, allConstraints);
    }
}

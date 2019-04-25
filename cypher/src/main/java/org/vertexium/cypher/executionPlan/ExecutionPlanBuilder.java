package org.vertexium.cypher.executionPlan;

import com.google.common.collect.ImmutableList;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExecutionPlanBuilder {
    public ExecutionPlan build(VertexiumCypherQueryContext ctx, CypherStatement statement) {
        return new ExecutionPlan(visitStatement(ctx, statement));
    }

    private ExecutionStep visitStatement(VertexiumCypherQueryContext ctx, CypherStatement statement) {
        return visitQueryOrUnion(ctx, statement.getQuery());
    }

    private ExecutionStep visitQueryOrUnion(VertexiumCypherQueryContext ctx, CypherAstBase query) {
        if (query instanceof CypherQuery) {
            return visitQuery(ctx, (CypherQuery) query);
        } else if (query instanceof CypherUnion) {
            return visitUnion(ctx, (CypherUnion) query);
        } else {
            throw new VertexiumCypherNotImplemented("unhandled query type: " + query.getClass().getName());
        }
    }

    private ExecutionStep visitUnion(VertexiumCypherQueryContext ctx, CypherUnion union) {
        return new UnionExecutionStep(
            union.isAll(),
            visitQueryOrUnion(ctx, union.getLeft()),
            visitQueryOrUnion(ctx, union.getRight())
        );
    }

    private ExecutionStep visitQuery(VertexiumCypherQueryContext ctx, CypherQuery query) {
        SeriesExecutionStep executionPlan = new SeriesExecutionStep();
        ImmutableList<CypherClause> clauses = query.getClauses();
        for (int i = 0; i < clauses.size(); i++) {
            CypherClause clause = clauses.get(i);
            if (clause instanceof CypherCreateClause) {
                executionPlan.addChildStep(visitCreateClause(ctx, (CypherCreateClause) clause));
            } else if (clause instanceof CypherMatchClause) {
                executionPlan.addChildStep(visitMatchClause(ctx, (CypherMatchClause) clause));
            } else if (clause instanceof CypherReturnClause) {
                executionPlan.addChildStep(visitReturnClause(ctx, (CypherReturnClause) clause));
            } else if (clause instanceof CypherUnwindClause) {
                executionPlan.addChildStep(visitUnwindClause(ctx, (CypherUnwindClause) clause));
            } else if (clause instanceof CypherDeleteClause) {
                executionPlan.addChildStep(visitDeleteClause(ctx, (CypherDeleteClause) clause));
            } else if (clause instanceof CypherSetClause) {
                executionPlan.addChildSteps(visitSetClause(ctx, (CypherSetClause) clause));
            } else if (clause instanceof CypherRemoveClause) {
                executionPlan.addChildSteps(visitRemoveClause(ctx, (CypherRemoveClause) clause));
            } else if (clause instanceof CypherWithClause) {
                executionPlan.addChildStep(visitWithClause(ctx, (CypherWithClause) clause));
            } else if (clause instanceof CypherMergeClause) {
                executionPlan.addChildStep(visitMergeClause(ctx, (CypherMergeClause) clause));
            } else {
                throw new VertexiumCypherNotImplemented("unhandled clause type (" + clause.getClass().getName() + "): " + clause);
            }
        }

        if (!(clauses.get(clauses.size() - 1) instanceof CypherReturnClause)) {
            executionPlan.addChildStep(new NoReturnValueExecutionStep());
        }

        return executionPlan;
    }

    private ExecutionStep visitMergeClause(VertexiumCypherQueryContext ctx, CypherMergeClause clause) {
        CypherPatternPart patternPart = clause.getPatternPart();
        for (CypherElementPattern elementPattern : patternPart.getElementPatterns()) {
            String resultName = elementPattern.getName();
            if (resultName == null) {
                elementPattern.setName(UUID.randomUUID().toString());
            }
        }
        return new SeriesExecutionStep(
            new MatchExecutionStep(
                new PatternPartExecutionStep[]{
                    visitPatternPart(ctx, true, patternPart)
                },
                null
            ),
            visitCreateClausePatternPart(ctx, patternPart, clause.getMergeActions())
        );
    }

    private ExecutionStep visitWithClause(VertexiumCypherQueryContext ctx, CypherWithClause clause) {
        return new WithClauseExecutionStep(
            visitReturnBody(ctx, clause.isDistinct(), clause.getReturnBody()),
            clause.getWhere() == null ? null : visitWhereExpression(ctx, clause.getWhere())
        );
    }

    private Stream<ExecutionStep> visitRemoveClause(VertexiumCypherQueryContext ctx, CypherRemoveClause clause) {
        return clause.getRemoveItems().stream()
            .map(removeItem -> visitRemoveItem(ctx, removeItem));
    }

    private ExecutionStep visitRemoveItem(VertexiumCypherQueryContext ctx, CypherRemoveItem removeItem) {
        if (removeItem instanceof CypherRemoveLabelItem) {
            CypherRemoveLabelItem removeLabelItem = (CypherRemoveLabelItem) removeItem;
            return new RemoveLabelItemExecutionStep(
                visitExpression(ctx, removeLabelItem.getVariable()),
                removeLabelItem.getLabelNames()
            );
        }
        throw new VertexiumCypherNotImplemented("remove item type: " + removeItem.getClass().getName());
    }

    private Stream<ExecutionStep> visitSetClause(VertexiumCypherQueryContext ctx, CypherSetClause clause) {
        return clause.getSetItems().stream()
            .map(setItem -> visitSetItem(ctx, setItem));
    }

    private ExecutionStep visitSetItem(VertexiumCypherQueryContext ctx, CypherSetItem setItem) {
        return new SetItemExecutionStep(
            visitExpression(ctx, setItem.getLeft()),
            setItem.getOp(),
            visitExpression(ctx, setItem.getRight())
        );
    }

    private ExecutionStep visitDeleteClause(VertexiumCypherQueryContext ctx, CypherDeleteClause clause) {
        return new SeriesExecutionStep(
            clause.getExpressions().stream()
                .map(expression -> new DeleteClauseExecutionStep(
                    clause.isDetach(),
                    visitExpression(ctx, expression)
                ))
                .toArray(ExecutionStep[]::new)
        );
    }

    private ExecutionStep visitUnwindClause(VertexiumCypherQueryContext ctx, CypherUnwindClause clause) {
        return new UnwindClauseExecutionStep(
            clause.getName(),
            visitExpression(ctx, clause.getExpression())
        );
    }

    private ReturnExecutionStep visitReturnClause(VertexiumCypherQueryContext ctx, CypherReturnClause clause) {
        boolean distinct = clause.isDistinct();
        CypherReturnBody returnBody = clause.getReturnBody();
        return visitReturnBody(ctx, distinct, returnBody);
    }

    private ReturnExecutionStep visitReturnBody(VertexiumCypherQueryContext ctx, boolean distinct, CypherReturnBody returnBody) {
        return new ReturnExecutionStep(
            distinct,
            returnBody.getReturnItems().stream()
                .map(returnItem -> new ReturnPartExecutionStep(
                    returnItem.getAlias(),
                    returnItem.getOriginalText(),
                    returnItem.getOriginalText().equals("*")
                        ? null
                        : visitExpression(ctx, returnItem.getExpression())
                ))
                .collect(Collectors.toList()),
            returnBody.getSkip() == null ? null : visitExpression(ctx, returnBody.getSkip().getExpression()),
            returnBody.getLimit() == null ? null : visitExpression(ctx, returnBody.getLimit().getExpression()),
            returnBody.getOrder() == null ? null : visitSortItems(ctx, returnBody.getOrder().getSortItems())
        );
    }

    private List<SortItemExecutionStep> visitSortItems(VertexiumCypherQueryContext ctx, List<CypherSortItem> sortItems) {
        return sortItems.stream().map(si -> visitSortItem(ctx, si)).collect(Collectors.toList());
    }

    private SortItemExecutionStep visitSortItem(VertexiumCypherQueryContext ctx, CypherSortItem sortItem) {
        return new SortItemExecutionStep(
            sortItem.getDirection(),
            visitExpression(ctx, sortItem.getExpression()),
            sortItem.getExpressionText()
        );
    }

    public ExecutionStepWithResultName visitExpression(VertexiumCypherQueryContext ctx, CypherAstBase expression) {
        return visitExpression(ctx, UUID.randomUUID().toString(), expression);
    }

    public ExecutionStepWithResultName visitExpression(VertexiumCypherQueryContext ctx, String resultName, CypherAstBase expression) {
        if (expression instanceof CypherVariable) {
            return visitVariable(ctx, resultName, (CypherVariable) expression);
        } else if (expression instanceof CypherBinaryExpression) {
            return visitBinaryExpression(ctx, resultName, (CypherBinaryExpression) expression);
        } else if (expression instanceof CypherComparisonExpression) {
            return visitComparisonExpression(ctx, resultName, (CypherComparisonExpression) expression);
        } else if (expression instanceof CypherLookup) {
            return visitLookup(ctx, resultName, (CypherLookup) expression);
        } else if (expression instanceof CypherLiteral) {
            if (expression instanceof CypherListLiteral) {
                return visitListLiteral(ctx, resultName, (CypherListLiteral) expression);
            }
            return visitLiteral(ctx, resultName, (CypherLiteral) expression);
        } else if (expression instanceof CypherFunctionInvocation) {
            return visitFunctionInvocation(ctx, resultName, (CypherFunctionInvocation) expression);
        } else if (expression instanceof CypherUnaryExpression) {
            return visitUnaryExpression(ctx, resultName, (CypherUnaryExpression) expression);
        } else if (expression instanceof CypherNegateExpression) {
            return visitNegateExpression(ctx, resultName, (CypherNegateExpression) expression);
        } else if (expression instanceof CypherArrayAccess) {
            return visitArrayAccess(ctx, resultName, (CypherArrayAccess) expression);
        } else if (expression instanceof CypherNameParameter) {
            return visitNameParameter(ctx, resultName, (CypherNameParameter) expression);
        } else if (expression instanceof CypherIsNotNull) {
            return visitIsNotNull(ctx, resultName, (CypherIsNotNull) expression);
        } else if (expression instanceof CypherIsNull) {
            return visitIsNull(ctx, resultName, (CypherIsNull) expression);
        } else if (expression instanceof CypherIn) {
            return visitIn(ctx, resultName, (CypherIn) expression);
        } else if (expression instanceof CypherListComprehension) {
            return visitListComprehension(ctx, resultName, (CypherListComprehension) expression);
        } else if (expression instanceof CypherStringMatch) {
            return visitStringMatch(ctx, resultName, (CypherStringMatch) expression);
        } else if (expression instanceof CypherIndexedParameter) {
            return visitIndexedParameter(ctx, resultName, (CypherIndexedParameter) expression);
        }
        throw new VertexiumCypherNotImplemented("expression: " + expression + " " + (expression == null ? "null" : expression.getClass().getName()));
    }

    private ExecutionStepWithResultName visitIndexedParameter(VertexiumCypherQueryContext ctx, String resultName, CypherIndexedParameter expression) {
        return new IndexedParameterExecutionStep(resultName, expression.getIndex());
    }

    private ExecutionStepWithResultName visitStringMatch(VertexiumCypherQueryContext ctx, String resultName, CypherStringMatch expression) {
        ExecutionStepWithResultName string = visitExpression(ctx, expression.getStringExpression());
        ExecutionStepWithResultName value = visitExpression(ctx, expression.getValueExpression());
        ExecutionStepWithResultName[] args = new ExecutionStepWithResultName[]{
            value,
            string
        };
        String functionName;
        switch (expression.getOp()) {
            case CONTAINS:
                functionName = "contains";
                break;
            case STARTS_WITH:
                functionName = "startsWith";
                break;
            case ENDS_WITH:
                functionName = "endsWith";
                break;
            default:
                throw new VertexiumCypherNotImplemented("Unhandled string match: " + expression.getOp());
        }
        return ctx.createFunctionExecutionStep(functionName, resultName, false, args);
    }

    private ExecutionStepWithResultName visitListComprehension(VertexiumCypherQueryContext ctx, String resultName, CypherListComprehension expression) {
        CypherFilterExpression filterExpression = expression.getFilterExpression();
        return new ListComprehensionExecutionStep(
            resultName,
            filterExpression.getIdInCol().getVariable().getName(),
            visitExpression(ctx, filterExpression.getIdInCol().getExpression()),
            filterExpression.getWhere() == null ? null : visitExpression(ctx, filterExpression.getWhere()),
            expression.getExpression() == null ? null : visitExpression(ctx, expression.getExpression())
        );
    }

    private ExecutionStepWithResultName visitIn(VertexiumCypherQueryContext ctx, String resultName, CypherIn expression) {
        return new InExecutionStep(
            resultName,
            visitExpression(ctx, expression.getValueExpression()),
            visitExpression(ctx, expression.getArrayExpression())
        );
    }

    private ExecutionStepWithResultName visitIsNotNull(VertexiumCypherQueryContext ctx, String resultName, CypherIsNotNull expression) {
        ExecutionStepWithResultName[] args = new ExecutionStepWithResultName[]{
            visitExpression(ctx, expression.getValueExpression())
        };
        return ctx.createFunctionExecutionStep("isNotNull", resultName, false, args);
    }

    private ExecutionStepWithResultName visitIsNull(VertexiumCypherQueryContext ctx, String resultName, CypherIsNull expression) {
        ExecutionStepWithResultName[] args = new ExecutionStepWithResultName[]{
            visitExpression(ctx, expression.getValueExpression())
        };
        return ctx.createFunctionExecutionStep("isNull", resultName, false, args);
    }

    private ExecutionStepWithResultName visitNameParameter(VertexiumCypherQueryContext ctx, String resultName, CypherNameParameter expression) {
        return new NameParameterExecutionStep(resultName, expression.getParameterName());
    }

    private ExecutionStepWithResultName visitArrayAccess(VertexiumCypherQueryContext ctx, String resultName, CypherArrayAccess expression) {
        return new ArrayAccessExecutionStep(
            resultName,
            visitExpression(ctx, expression.getArrayExpression()),
            visitExpression(ctx, expression.getIndexExpression())
        );
    }

    private ExecutionStepWithResultName visitNegateExpression(VertexiumCypherQueryContext ctx, String resultName, CypherNegateExpression expression) {
        ExecutionStepWithResultName[] args = new ExecutionStepWithResultName[]{
            visitExpression(ctx, expression.getValue())
        };
        return ctx.createFunctionExecutionStep("negate", resultName, false, args);
    }

    private UnaryExpressionExecutionStep visitUnaryExpression(VertexiumCypherQueryContext ctx, String resultName, CypherUnaryExpression expression) {
        return new UnaryExpressionExecutionStep(
            resultName,
            expression.getOp(),
            visitExpression(ctx, expression.getExpression())
        );
    }

    private ExecutionStepWithResultName visitFunctionInvocation(VertexiumCypherQueryContext ctx, String resultName, CypherFunctionInvocation expression) {
        CypherAstBase[] arguments = expression.getArguments();
        ExecutionStepWithResultName[] argumentsExecutionStep = new ExecutionStepWithResultName[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            argumentsExecutionStep[i] = visitExpression(ctx, arguments[i]);
        }
        return ctx.createFunctionExecutionStep(expression.getFunctionName(), resultName, expression.isDistinct(), argumentsExecutionStep);
    }

    private ListLiteralExecutionStep visitListLiteral(VertexiumCypherQueryContext ctx, String resultName, CypherListLiteral<Object> expression) {
        ExecutionStepWithResultName[] elements = expression.stream()
            .map(e -> {
                if (e instanceof CypherAstBase) {
                    return visitExpression(ctx, (CypherAstBase) e);
                } else {
                    throw new VertexiumCypherNotImplemented("unexpected list item: " + e.getClass().getName());
                }
            })
            .toArray(ExecutionStepWithResultName[]::new);
        return new ListLiteralExecutionStep(resultName, elements);
    }

    private ExecutionStepWithResultName visitLiteral(VertexiumCypherQueryContext ctx, String resultName, CypherLiteral expression) {
        Object value = expression.getValue();
        if (value instanceof Map) {
            Map<String, CypherAstBase> map = (Map<String, CypherAstBase>) value;
            return new MapLiteralExecutionStep(
                resultName,
                map.entrySet().stream()
                    .map(entry -> visitExpression(ctx, entry.getKey(), entry.getValue()))
                    .toArray(ExecutionStepWithResultName[]::new)
            );
        }
        return new LiteralExecutionStep(resultName, value);
    }

    private LookupExecutionStep visitLookup(VertexiumCypherQueryContext ctx, String resultName, CypherLookup expression) {
        return new LookupExecutionStep(
            resultName,
            visitExpression(ctx, expression.getAtom()),
            expression.getProperty(),
            expression.getLabels()
        );
    }

    private ComparisonExpressionExecutionStep visitComparisonExpression(VertexiumCypherQueryContext ctx, String resultName, CypherComparisonExpression expression) {
        return new ComparisonExpressionExecutionStep(
            resultName,
            expression.getOp(),
            visitExpression(ctx, expression.getLeft()),
            visitExpression(ctx, expression.getRight())
        );
    }

    private BinaryExpressionExecutionStep visitBinaryExpression(VertexiumCypherQueryContext ctx, String resultName, CypherBinaryExpression expression) {
        return new BinaryExpressionExecutionStep(
            resultName,
            visitExpression(ctx, expression.getLeft()),
            visitExpression(ctx, expression.getRight()),
            expression.getOp()
        );
    }

    private ExecutionStepWithResultName visitVariable(VertexiumCypherQueryContext ctx, String resultName, CypherVariable expression) {
        return new GetVariableExecutionStep(resultName, expression.getName());
    }

    private MatchExecutionStep visitMatchClause(VertexiumCypherQueryContext ctx, CypherMatchClause clause) {
        return new MatchExecutionStep(
            clause.getPatternParts().stream()
                .map(patternPart -> visitPatternPart(ctx, clause.isOptional(), patternPart))
                .toArray(PatternPartExecutionStep[]::new),
            clause.getWhereExpression() == null ? null : visitWhereExpression(ctx, clause.getWhereExpression())
        );
    }

    private PatternPartExecutionStep visitPatternPart(VertexiumCypherQueryContext ctx, boolean optional, CypherPatternPart patternPart) {
        CypherListLiteral<CypherElementPattern> elementPatterns = patternPart.getElementPatterns();
        List<MatchPartExecutionStep> steps = elementPatterns.stream()
            .map(elementPattern -> {
                String resultName = elementPattern.getName();
                if (resultName == null) {
                    resultName = UUID.randomUUID().toString();
                }
                return createMatchPartExecutionStep(ctx, resultName, optional, elementPattern);
            })
            .collect(Collectors.toList());

        MatchPartExecutionStep[] matchPartExecutionSteps = new MatchPartExecutionStep[steps.size()];
        for (int i = 0; i < steps.size(); i++) {
            MatchPartExecutionStep step = steps.get(i);
            if (i > 0) {
                step.addConnectedStep(steps.get(i - 1));
            }
            if (i < steps.size() - 1) {
                step.addConnectedStep(steps.get(i + 1));
            }
            matchPartExecutionSteps[i] = step;
        }
        return new PatternPartExecutionStep(
            patternPart.getName(),
            matchPartExecutionSteps
        );
    }

    private MatchPartExecutionStep createMatchPartExecutionStep(
        VertexiumCypherQueryContext ctx,
        String resultName,
        boolean optional,
        CypherElementPattern elementPattern
    ) {
        if (elementPattern instanceof CypherNodePattern) {
            CypherNodePattern nodePattern = (CypherNodePattern) elementPattern;
            return new MatchNodePartExecutionStep(
                elementPattern.getName(),
                resultName,
                optional,
                nodePattern.getLabelNames().stream().map(CypherLiteral::getValue).collect(Collectors.toSet()),
                visitPropertyMap(ctx, nodePattern.getPropertiesMap())
            );
        } else if (elementPattern instanceof CypherRelationshipPattern) {
            CypherRelationshipPattern relPattern = (CypherRelationshipPattern) elementPattern;
            return new MatchRelationshipPartExecutionStep(
                elementPattern.getName(),
                resultName,
                optional,
                relPattern.getRelTypeNames() == null
                    ? new ArrayList<>()
                    : relPattern.getRelTypeNames().stream().map(CypherLiteral::getValue).collect(Collectors.toList()),
                relPattern.getDirection(),
                relPattern.getRange(),
                visitPropertyMap(ctx, relPattern.getPropertiesMap())
            );
        } else {
            throw new VertexiumCypherException("Expected a node or relationship pattern found " + elementPattern.getClass().getName());
        }
    }

    private WhereExecutionStep visitWhereExpression(VertexiumCypherQueryContext ctx, CypherAstBase whereExpression) {
        return new WhereExecutionStep(visitExpression(ctx, whereExpression));
    }

    private JoinExecutionStep visitCreateClause(VertexiumCypherQueryContext ctx, CypherCreateClause clause) {
        JoinExecutionStep executionPlan = new JoinExecutionStep(true);
        for (CypherPatternPart patternPart : clause.getPatternParts()) {
            executionPlan.addChildStep(visitCreateClausePatternPart(ctx, patternPart, null));
        }
        return executionPlan;
    }

    private CreatePatternExecutionStep visitCreateClausePatternPart(
        VertexiumCypherQueryContext ctx,
        CypherPatternPart patternPart,
        List<CypherMergeAction> mergeActions
    ) {
        CypherListLiteral<CypherElementPattern> elementPatterns = patternPart.getElementPatterns();
        CreateElementPatternExecutionStep[] steps = new CreateElementPatternExecutionStep[elementPatterns.size()];
        List<CreateNodePatternExecutionStep> createNodeExecutionSteps = new ArrayList<>();
        List<CreateRelationshipPatternExecutionStep> createRelationshipExecutionSteps = new ArrayList<>();

        for (int i = 0; i < elementPatterns.size(); i++) {
            CypherElementPattern elementPattern = elementPatterns.get(i);
            if (elementPattern instanceof CypherNodePattern) {
                CreateNodePatternExecutionStep step = visitCreateNodePattern(ctx, (CypherNodePattern) elementPattern, mergeActions);
                createNodeExecutionSteps.add(step);
                steps[i] = step;
            } else if (elementPattern instanceof CypherRelationshipPattern) {
                // OK
            } else {
                throw new VertexiumCypherNotImplemented("Unhandled create pattern type: " + elementPattern.getClass().getName());
            }
        }

        for (int i = 0; i < elementPatterns.size(); i++) {
            CypherElementPattern elementPattern = elementPatterns.get(i);
            if (elementPattern instanceof CypherRelationshipPattern) {
                String left = steps[i - 1].getResultName();
                String right = steps[i + 1].getResultName();
                CreateRelationshipPatternExecutionStep step = visitCreateRelationshipPattern(
                    ctx,
                    (CypherRelationshipPattern) elementPattern,
                    left,
                    right,
                    mergeActions
                );
                steps[i] = step;
                createRelationshipExecutionSteps.add(step);
            }
        }

        return new CreatePatternExecutionStep(
            patternPart.getName(),
            createNodeExecutionSteps,
            createRelationshipExecutionSteps
        );
    }

    private CreateRelationshipPatternExecutionStep visitCreateRelationshipPattern(
        VertexiumCypherQueryContext ctx,
        CypherRelationshipPattern p,
        String leftNodeName,
        String rightNodeName,
        List<CypherMergeAction> mergeActions
    ) {
        return new CreateRelationshipPatternExecutionStep(
            p.getName() == null ? UUID.randomUUID().toString() : p.getName(),
            p.getRelTypeNames() == null
                ? new ArrayList<>()
                : p.getRelTypeNames().stream().map(CypherLiteral::getValue).collect(Collectors.toList()),
            p.getDirection(),
            leftNodeName,
            rightNodeName,
            visitPropertyMap(ctx, p.getPropertiesMap()),
            visitMergeActions(ctx, mergeActions)
        );
    }

    private CreateNodePatternExecutionStep visitCreateNodePattern(
        VertexiumCypherQueryContext ctx,
        CypherNodePattern p,
        List<CypherMergeAction> mergeActions
    ) {
        return new CreateNodePatternExecutionStep(
            p.getName() == null ? UUID.randomUUID().toString() : p.getName(),
            p.getLabelNames().stream().map(CypherLiteral::getValue).collect(Collectors.toList()),
            visitPropertyMap(ctx, p.getPropertiesMap()),
            visitMergeActions(ctx, mergeActions)
        );
    }

    private List<ExecutionStep> visitMergeActions(VertexiumCypherQueryContext ctx, List<CypherMergeAction> mergeActions) {
        if (mergeActions == null) {
            mergeActions = new ArrayList<>();
        }
        return mergeActions.stream()
            .flatMap(ma -> {
                Stream<ExecutionStep> setSteps = visitSetClause(ctx, ma.getSet());
                if (ma instanceof CypherMergeActionCreate) {
                    return setSteps.map(step -> new MergeActionExecutionStep(MergeActionExecutionStep.Type.CREATE, step));
                } else if (ma instanceof CypherMergeActionMatch) {
                    return setSteps.map(step -> new MergeActionExecutionStep(MergeActionExecutionStep.Type.MATCH, step));
                } else {
                    throw new VertexiumCypherNotImplemented("Unhandled merge action type: " + ma.getClass().getName());
                }
            })
            .collect(Collectors.toList());
    }

    private List<ExecutionStepWithResultName> visitPropertyMap(VertexiumCypherQueryContext ctx, CypherMapLiteral<String, CypherAstBase> propertiesMap) {
        List<ExecutionStepWithResultName> results = new ArrayList<>();
        for (Map.Entry<String, CypherAstBase> propertyEntry : propertiesMap.entrySet()) {
            ExecutionStepWithResultName propertyStep = visitExpression(ctx, propertyEntry.getKey(), propertyEntry.getValue());
            results.add(propertyStep);
        }
        return results;
    }
}

package org.vertexium.cypher.ast;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.CypherBaseVisitor;
import org.vertexium.cypher.CypherParser;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherSyntaxErrorException;
import org.vertexium.cypher.functions.CypherFunction;
import org.vertexium.util.StreamUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class CypherCstToAstVisitor extends CypherBaseVisitor<CypherAstBase> {
    private final CypherCompilerContext compilerContext;

    public CypherCstToAstVisitor() {
        this(new CypherCompilerContext());
    }

    public CypherCstToAstVisitor(CypherCompilerContext compilerContext) {
        this.compilerContext = compilerContext;
    }

    @Override
    public CypherStatement visitOC_Statement(CypherParser.OC_StatementContext ctx) {
        return new CypherStatement(visitQuery(ctx.oC_Query()));
    }

    public CypherAstBase visitQuery(CypherParser.OC_QueryContext ctx) {
        return visitRegularQuery(ctx.oC_RegularQuery());
    }

    public CypherAstBase visitRegularQuery(CypherParser.OC_RegularQueryContext ctx) {
        CypherQuery left = visitSingleQuery(ctx.oC_SingleQuery());
        if (ctx.oC_Union().size() > 0) {
            return visitUnions(left, ctx.oC_Union());
        }
        return left;
    }

    public CypherQuery visitSingleQuery(CypherParser.OC_SingleQueryContext ctx) {
        if (ctx.oC_MultiPartQuery() != null) {
            return visitOC_MultiPartQuery(ctx.oC_MultiPartQuery());
        } else if (ctx.oC_SinglePartQuery() != null) {
            return visitOC_SinglePartQuery(ctx.oC_SinglePartQuery());
        } else {
            throw new VertexiumCypherNotImplemented("Unhandled child: " + ctx.getText());
        }
    }

    @Override
    public CypherQuery visitOC_MultiPartQuery(CypherParser.OC_MultiPartQueryContext ctx) {
        return queryChildrenToCypherQuery(ctx.children);
    }

    @Override
    public CypherQuery visitOC_SinglePartQuery(CypherParser.OC_SinglePartQueryContext ctx) {
        return queryChildrenToCypherQuery(ctx.children);
    }

    private CypherQuery queryChildrenToCypherQuery(List<ParseTree> children) {
        return new CypherQuery(
            children.stream()
                .flatMap(c -> {
                    if (c instanceof CypherParser.OC_SinglePartQueryContext) {
                        return ((CypherParser.OC_SinglePartQueryContext) c).children.stream();
                    } else {
                        return Stream.of(c);
                    }
                })
                .filter(c -> c.getText().trim().length() > 0)
                .map(c -> {
                    if (c instanceof CypherParser.OC_ReadingClauseContext) {
                        return visitOC_ReadingClause((CypherParser.OC_ReadingClauseContext) c);
                    } else if (c instanceof CypherParser.OC_UpdatingClauseContext) {
                        return visitOC_UpdatingClause((CypherParser.OC_UpdatingClauseContext) c);
                    } else if (c instanceof CypherParser.OC_ReturnContext) {
                        return visitOC_Return((CypherParser.OC_ReturnContext) c);
                    } else if (c instanceof CypherParser.OC_WithContext) {
                        return visitOC_With((CypherParser.OC_WithContext) c);
                    } else {
                        throw new VertexiumCypherNotImplemented("Unhandled child (" + c.getText() + "): " + c.getClass().getName());
                    }
                })
                .collect(StreamUtils.toImmutableList())
        );
    }

    @Override
    public CypherClause visitOC_UpdatingClause(CypherParser.OC_UpdatingClauseContext ctx) {
        if (ctx.oC_Create() != null) {
            return visitOC_Create(ctx.oC_Create());
        } else if (ctx.oC_Merge() != null) {
            return visitOC_Merge(ctx.oC_Merge());
        } else if (ctx.oC_Delete() != null) {
            return visitOC_Delete(ctx.oC_Delete());
        } else if (ctx.oC_Set() != null) {
            return visitOC_Set(ctx.oC_Set());
        } else if (ctx.oC_Remove() != null) {
            return visitOC_Remove(ctx.oC_Remove());
        } else {
            throw new VertexiumCypherNotImplemented("Unhandled child: " + ctx.getText());
        }
    }

    @Override
    public CypherClause visitOC_ReadingClause(CypherParser.OC_ReadingClauseContext ctx) {
        if (ctx.oC_Match() != null) {
            return visitOC_Match(ctx.oC_Match());
        } else if (ctx.oC_Unwind() != null) {
            return visitOC_Unwind(ctx.oC_Unwind());
        } else if (ctx.oC_InQueryCall() != null) {
            return visitOC_InQueryCall(ctx.oC_InQueryCall());
        } else {
            throw new VertexiumCypherNotImplemented("Unhandled child: " + ctx.getText());
        }
    }

    @Override
    public CypherClause visitOC_InQueryCall(CypherParser.OC_InQueryCallContext ctx) {
        throw new VertexiumCypherNotImplemented("visitOC_InQueryCall: " + ctx.getText());
    }

    @Override
    public CypherCreateClause visitOC_Create(CypherParser.OC_CreateContext ctx) {
        ImmutableList<CypherPatternPart> patternParts = ctx.oC_Pattern().oC_PatternPart().stream()
            .map(this::visitOC_PatternPart)
            .collect(StreamUtils.toImmutableList());
        return new CypherCreateClause(patternParts);
    }

    @Override
    public CypherReturnClause visitOC_Return(CypherParser.OC_ReturnContext ctx) {
        boolean distinct = ctx.DISTINCT() != null;
        return new CypherReturnClause(distinct, visitOC_ReturnBody(ctx.oC_ReturnBody()));
    }

    @Override
    public CypherReturnBody visitOC_ReturnBody(CypherParser.OC_ReturnBodyContext ctx) {
        CypherParser.OC_OrderContext order = ctx.oC_Order();
        CypherParser.OC_LimitContext limit = ctx.oC_Limit();
        CypherParser.OC_SkipContext skip = ctx.oC_Skip();
        return new CypherReturnBody(
            visitOC_ReturnItems(ctx.oC_ReturnItems()),
            (order == null) ? null : visitOC_Order(order),
            (limit == null) ? null : visitOC_Limit(limit),
            (skip == null) ? null : visitOC_Skip(skip)
        );
    }

    @Override
    public CypherMatchClause visitOC_Match(CypherParser.OC_MatchContext ctx) {
        boolean optional = ctx.OPTIONAL() != null;
        CypherListLiteral<CypherPatternPart> patternParts = visitOC_Pattern(ctx.oC_Pattern());
        CypherAstBase whereExpression = visitOC_Where(ctx.oC_Where());
        return new CypherMatchClause(optional, patternParts, whereExpression);
    }

    @Override
    public CypherListLiteral<CypherPatternPart> visitOC_Pattern(CypherParser.OC_PatternContext ctx) {
        return ctx.oC_PatternPart().stream()
            .map(this::visitOC_PatternPart)
            .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherPatternPart visitOC_PatternPart(CypherParser.OC_PatternPartContext ctx) {
        String name = visitVariableString(ctx.oC_Variable());
        CypherListLiteral<CypherElementPattern> elementPatterns = visitOC_AnonymousPatternPart(ctx.oC_AnonymousPatternPart());
        return new CypherPatternPart(name, elementPatterns);
    }

    @Override
    public CypherListLiteral<CypherElementPattern> visitOC_AnonymousPatternPart(CypherParser.OC_AnonymousPatternPartContext ctx) {
        return visitOC_PatternElement(ctx.oC_PatternElement());
    }

    @Override
    public CypherListLiteral<CypherElementPattern> visitOC_PatternElement(CypherParser.OC_PatternElementContext ctx) {
        // unwind parenthesis
        if (ctx.oC_PatternElement() != null) {
            return visitOC_PatternElement(ctx.oC_PatternElement());
        }

        List<CypherElementPattern> list = new ArrayList<>();
        CypherNodePattern nodePattern = visitOC_NodePattern(ctx.oC_NodePattern());
        list.add(nodePattern);
        list.addAll(visitPatternElementChainList(nodePattern, ctx.oC_PatternElementChain()));
        return new CypherListLiteral<>(list);
    }

    private List<CypherElementPattern> visitPatternElementChainList(
        CypherNodePattern previousNodePattern,
        List<CypherParser.OC_PatternElementChainContext> patternElementChainList
    ) {
        List<CypherElementPattern> list = new ArrayList<>();
        for (CypherParser.OC_PatternElementChainContext chainContext : patternElementChainList) {
            CypherRelationshipPattern relationshipPattern = visitOC_RelationshipPattern(chainContext.oC_RelationshipPattern());
            relationshipPattern.setPreviousNodePattern(previousNodePattern);
            list.add(relationshipPattern);

            CypherNodePattern nodePattern = visitOC_NodePattern(chainContext.oC_NodePattern());
            relationshipPattern.setNextNodePattern(nodePattern);
            list.add(nodePattern);

            previousNodePattern = nodePattern;
        }
        return list;
    }

    @Override
    public CypherNodePattern visitOC_NodePattern(CypherParser.OC_NodePatternContext ctx) {
        return new CypherNodePattern(
            visitVariableString(ctx.oC_Variable()),
            visitOC_Properties(ctx.oC_Properties()),
            visitOC_NodeLabels(ctx.oC_NodeLabels())
        );
    }

    @Override
    public CypherRelationshipPattern visitOC_RelationshipPattern(CypherParser.OC_RelationshipPatternContext ctx) {
        CypherParser.OC_RelationshipDetailContext relationshipDetail = ctx.oC_RelationshipDetail();
        String name;
        CypherListLiteral<CypherRelTypeName> relTypeNames;
        CypherMapLiteral<String, CypherAstBase> properties;
        CypherRangeLiteral range;

        if (relationshipDetail == null) {
            name = null;
            relTypeNames = null;
            properties = null;
            range = null;
        } else {
            if (relationshipDetail.oC_RangeLiteral() != null) {
                range = visitOC_RangeLiteral(relationshipDetail.oC_RangeLiteral());
            } else {
                range = null;
            }
            name = visitVariableString(relationshipDetail.oC_Variable());
            if (relationshipDetail.oC_RelationshipTypes() == null) {
                relTypeNames = null;
            } else {
                relTypeNames = visitOC_RelationshipTypes(relationshipDetail.oC_RelationshipTypes());
            }
            properties = visitOC_Properties(relationshipDetail.oC_Properties());
        }

        CypherDirection direction = getDirectionFromRelationshipPattern(ctx);
        return new CypherRelationshipPattern(name, relTypeNames, properties, range, direction);
    }

    private static CypherDirection getDirectionFromRelationshipPattern(CypherParser.OC_RelationshipPatternContext relationshipPatternContext) {
        if (relationshipPatternContext.oC_LeftArrowHead() != null && relationshipPatternContext.oC_RightArrowHead() != null) {
            return CypherDirection.BOTH;
        }
        if (relationshipPatternContext.oC_LeftArrowHead() != null) {
            return CypherDirection.IN;
        }
        if (relationshipPatternContext.oC_RightArrowHead() != null) {
            return CypherDirection.OUT;
        }
        return CypherDirection.UNSPECIFIED;
    }

    @Override
    public CypherMapLiteral<String, CypherAstBase> visitOC_Properties(CypherParser.OC_PropertiesContext ctx) {
        if (ctx == null) {
            return null;
        }
        //noinspection unchecked
        return (CypherMapLiteral<String, CypherAstBase>) super.visitOC_Properties(ctx);
    }

    @Override
    public CypherMapLiteral<String, CypherAstBase> visitOC_MapLiteral(CypherParser.OC_MapLiteralContext ctx) {
        List<CypherParser.OC_PropertyKeyNameContext> keys = ctx.oC_PropertyKeyName();
        List<CypherParser.OC_ExpressionContext> values = ctx.oC_Expression();
        Map<String, CypherAstBase> result = new HashMap<>();
        for (int i = 0, keysSize = keys.size(); i < keysSize; i++) {
            String key = visitOC_PropertyKeyName(keys.get(i)).getValue();
            CypherAstBase value = visitOC_Expression(values.get(i));
            result.put(key, value);
        }
        return new CypherMapLiteral<>(result);
    }

    @Override
    public CypherString visitOC_PropertyKeyName(CypherParser.OC_PropertyKeyNameContext ctx) {
        return visitOC_SchemaName(ctx.oC_SchemaName());
    }

    @Override
    public CypherListLiteral<CypherLabelName> visitOC_NodeLabels(CypherParser.OC_NodeLabelsContext ctx) {
        if (ctx == null) {
            return new CypherListLiteral<>();
        }
        return ctx.oC_NodeLabel().stream()
            .map(nl -> visitOC_LabelName(nl.oC_LabelName()))
            .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherLabelName visitOC_LabelName(CypherParser.OC_LabelNameContext ctx) {
        return new CypherLabelName(visitOC_SchemaName(ctx.oC_SchemaName()).getValue());
    }

    @Override
    public CypherAstBase visitOC_PatternElementChain(CypherParser.OC_PatternElementChainContext ctx) {
        throw new VertexiumException("should not be called, see visitPatternElementChainList");
    }

    @Override
    public CypherUnwindClause visitOC_Unwind(CypherParser.OC_UnwindContext ctx) {
        String name = visitVariableString(ctx.oC_Variable());
        CypherAstBase expression = visitOC_Expression(ctx.oC_Expression());
        return new CypherUnwindClause(name, expression);
    }

    @Override
    public CypherWithClause visitOC_With(CypherParser.OC_WithContext ctx) {
        boolean distinct = ctx.DISTINCT() != null;
        CypherReturnBody returnBody = visitOC_ReturnBody(ctx.oC_ReturnBody());
        CypherAstBase where = visitOC_Where(ctx.oC_Where());
        return new CypherWithClause(distinct, returnBody, where);
    }

    @Override
    public CypherMergeClause visitOC_Merge(CypherParser.OC_MergeContext ctx) {
        CypherPatternPart patternPart = visitOC_PatternPart(ctx.oC_PatternPart());
        List<CypherMergeAction> mergeActions = ctx.oC_MergeAction().stream()
            .map(this::visitOC_MergeAction)
            .collect(Collectors.toList());
        return new CypherMergeClause(
            patternPart,
            mergeActions
        );
    }

    @Override
    public CypherAstBase visitOC_Where(CypherParser.OC_WhereContext ctx) {
        if (ctx == null) {
            return null;
        }
        return visitOC_Expression(ctx.oC_Expression());
    }

    public CypherListLiteral<CypherAstBase> visitExpressions(Iterable<CypherParser.OC_ExpressionContext> expressionContexts) {
        return stream(expressionContexts)
            .map(this::visitOC_Expression)
            .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherAstBase visitOC_Expression(CypherParser.OC_ExpressionContext ctx) {
        return visitOC_OrExpression(ctx.oC_OrExpression());
    }

    @Override
    public CypherAstBase visitOC_OrExpression(CypherParser.OC_OrExpressionContext ctx) {
        List<CypherParser.OC_XorExpressionContext> children = ctx.oC_XorExpression();
        if (children.size() == 1) {
            return visitOC_XorExpression(children.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitOC_XorExpression);
    }

    @Override
    public CypherAstBase visitOC_XorExpression(CypherParser.OC_XorExpressionContext ctx) {
        List<CypherParser.OC_AndExpressionContext> children = ctx.oC_AndExpression();
        if (children.size() == 1) {
            return visitOC_AndExpression(children.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitOC_AndExpression);
    }

    @Override
    public CypherAstBase visitOC_AndExpression(CypherParser.OC_AndExpressionContext ctx) {
        List<CypherParser.OC_NotExpressionContext> children = ctx.oC_NotExpression();
        if (children.size() == 1) {
            return visitOC_NotExpression(children.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitOC_NotExpression);
    }

    private <T extends ParseTree> CypherBinaryExpression toBinaryExpressions(List<ParseTree> children, Function<T, CypherAstBase> itemTransform) {
        CypherAstBase left = null;
        CypherBinaryExpression.Op op = null;
        for (int i = 0; i < children.size(); i++) {
            ParseTree child = children.get(i);
            if (child instanceof TerminalNode) {
                CypherBinaryExpression.Op newOp = CypherBinaryExpression.Op.parseOrNull(child.getText());
                if (newOp != null) {
                    if (op == null) {
                        op = newOp;
                    } else {
                        throw new VertexiumException("unexpected op, found too many ops in a row");
                    }
                }
            } else {
                //noinspection unchecked
                CypherAstBase childObj = itemTransform.apply((T) child);
                if (left == null) {
                    left = childObj;
                } else {
                    if (op == null) {
                        throw new VertexiumException("unexpected binary expression. expected an op between expressions");
                    }
                    left = new CypherBinaryExpression(left, op, childObj);
                }
                op = null;
            }
        }
        return (CypherBinaryExpression) left;
    }

    @Override
    public CypherAstBase visitOC_NotExpression(CypherParser.OC_NotExpressionContext ctx) {
        if (ctx.NOT().size() % 2 == 0) {
            return visitOC_ComparisonExpression(ctx.oC_ComparisonExpression());
        } else {
            return new CypherUnaryExpression(CypherUnaryExpression.Op.NOT, visitOC_ComparisonExpression(ctx.oC_ComparisonExpression()));
        }
    }

    @Override
    public CypherAstBase visitOC_ComparisonExpression(CypherParser.OC_ComparisonExpressionContext ctx) {
        List<CypherParser.OC_PartialComparisonExpressionContext> partialComparisonExpressions = ctx.oC_PartialComparisonExpression();
        if (partialComparisonExpressions.size() == 0) {
            return visitOC_AddOrSubtractExpression(ctx.oC_AddOrSubtractExpression());
        }

        CypherAstBase left = visitOC_AddOrSubtractExpression(ctx.oC_AddOrSubtractExpression());
        String op = partialComparisonExpressions.get(0).children.get(0).getText();
        CypherAstBase right = visitOC_AddOrSubtractExpression(partialComparisonExpressions.get(0).oC_AddOrSubtractExpression());
        CypherComparisonExpression leftExpression = new CypherComparisonExpression(left, op, right);
        CypherAstBase rightExpression = visitOC_PartialComparisonExpression(right, 1, partialComparisonExpressions);
        if (rightExpression instanceof CypherTrueExpression) {
            return leftExpression;
        }
        return new CypherBinaryExpression(leftExpression, CypherBinaryExpression.Op.AND, rightExpression);
    }

    private CypherAstBase visitOC_PartialComparisonExpression(
        CypherAstBase left,
        int partialComparisonExpressionIndex,
        List<CypherParser.OC_PartialComparisonExpressionContext> partialComparisonExpressions
    ) {
        if (partialComparisonExpressionIndex >= partialComparisonExpressions.size()) {
            return new CypherTrueExpression();
        }
        String op = partialComparisonExpressions.get(partialComparisonExpressionIndex).children.get(0).getText();
        CypherAstBase right = visitOC_AddOrSubtractExpression(partialComparisonExpressions.get(partialComparisonExpressionIndex).oC_AddOrSubtractExpression());
        CypherComparisonExpression binLeft = new CypherComparisonExpression(left, op, right);
        CypherAstBase binRight = visitOC_PartialComparisonExpression(right, partialComparisonExpressionIndex + 1, partialComparisonExpressions);
        if (binRight instanceof CypherTrueExpression) {
            return binLeft;
        }
        return new CypherBinaryExpression(binLeft, CypherBinaryExpression.Op.AND, binRight);
    }

    @Override
    public CypherAstBase visitOC_AddOrSubtractExpression(CypherParser.OC_AddOrSubtractExpressionContext ctx) {
        List<CypherParser.OC_MultiplyDivideModuloExpressionContext> expression6s = ctx.oC_MultiplyDivideModuloExpression();
        if (expression6s.size() == 1) {
            return visitOC_MultiplyDivideModuloExpression(expression6s.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitOC_MultiplyDivideModuloExpression);
    }

    @Override
    public CypherAstBase visitOC_MultiplyDivideModuloExpression(CypherParser.OC_MultiplyDivideModuloExpressionContext ctx) {
        List<CypherParser.OC_PowerOfExpressionContext> expression5s = ctx.oC_PowerOfExpression();
        if (expression5s.size() == 1) {
            return visitOC_PowerOfExpression(expression5s.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitOC_PowerOfExpression);
    }

    @Override
    public CypherAstBase visitOC_PowerOfExpression(CypherParser.OC_PowerOfExpressionContext ctx) {
        List<CypherParser.OC_UnaryAddOrSubtractExpressionContext> expression4s = ctx.oC_UnaryAddOrSubtractExpression();
        if (expression4s.size() == 1) {
            return visitOC_UnaryAddOrSubtractExpression(expression4s.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitOC_UnaryAddOrSubtractExpression);
    }

    @Override
    public CypherAstBase visitOC_UnaryAddOrSubtractExpression(CypherParser.OC_UnaryAddOrSubtractExpressionContext ctx) {
        int neg = 0;
        for (ParseTree child : ctx.children) {
            if (child instanceof TerminalNode && child.getText().equals("-")) {
                neg++;
            }
        }
        CypherAstBase expr = visitOC_StringListNullOperatorExpression(ctx.oC_StringListNullOperatorExpression());
        if (neg % 2 == 1) {
            return new CypherNegateExpression(expr);
        } else {
            return expr;
        }
    }

    @Override
    public CypherAstBase visitOC_StringListNullOperatorExpression(CypherParser.OC_StringListNullOperatorExpressionContext ctx) {
        if (ctx.children.size() == 1) {
            return visitOC_PropertyOrLabelsExpression(ctx.oC_PropertyOrLabelsExpression());
        }
        return visitOC_StringListNullOperatorExpression(filterSpaces(ctx.children).collect(Collectors.toList()));
    }

    private Stream<ParseTree> filterSpaces(List<ParseTree> items) {
        return items.stream()
            .filter(item -> item.getText().trim().length() > 0);
    }

    private CypherAstBase visitOC_StringListNullOperatorExpression(List<ParseTree> children) {
        CypherAstBase valueExpression = visitOC_PropertyOrLabelsExpression((CypherParser.OC_PropertyOrLabelsExpressionContext) children.get(0));

        // is null - a IS NULL
        if (children.size() == 2 && children.get(1).getText().trim().equalsIgnoreCase("IS NULL")) {
            return new CypherIsNull(valueExpression);
        }

        // is not null - a IS NOT NULL
        if (children.size() == 2 && children.get(1).getText().trim().equalsIgnoreCase("IS NOT NULL")) {
            return new CypherIsNotNull(valueExpression);
        }

        // string operator
        if (children.size() == 2 && children.get(1) instanceof CypherParser.OC_StringOperatorExpressionContext) {
            CypherParser.OC_StringOperatorExpressionContext stringOpExpr = (CypherParser.OC_StringOperatorExpressionContext) children.get(1);

            // starts with - 'abc' STARTS WITH 'a'
            if (stringOpExpr.STARTS() != null && stringOpExpr.WITH() != null) {
                CypherAstBase stringExpression = visitOC_PropertyOrLabelsExpression(stringOpExpr.oC_PropertyOrLabelsExpression());
                return new CypherStringMatch(valueExpression, stringExpression, CypherStringMatch.Op.STARTS_WITH);
            }

            // ends with - 'abc' ENDS WITH 'a'
            if (stringOpExpr.ENDS() != null && stringOpExpr.WITH() != null) {
                CypherAstBase stringExpression = visitOC_PropertyOrLabelsExpression(stringOpExpr.oC_PropertyOrLabelsExpression());
                return new CypherStringMatch(valueExpression, stringExpression, CypherStringMatch.Op.ENDS_WITH);
            }

            // contains - 'abc' CONTAINS 'a'
            if (stringOpExpr.CONTAINS() != null) {
                CypherAstBase stringExpression = visitOC_PropertyOrLabelsExpression((CypherParser.OC_PropertyOrLabelsExpressionContext) stringOpExpr.oC_PropertyOrLabelsExpression());
                return new CypherStringMatch(valueExpression, stringExpression, CypherStringMatch.Op.CONTAINS);
            }
        }

        // list operator
        if (children.size() >= 2 && children.get(1) instanceof CypherParser.OC_ListOperatorExpressionContext) {
            CypherParser.OC_ListOperatorExpressionContext listOp = (CypherParser.OC_ListOperatorExpressionContext) children.get(1);

            // item in list - 'a' IN [ 1, 2, 3 ]
            if (listOp.IN() != null) {
                CypherAstBase arrExpression = visitOC_PropertyOrLabelsExpression(listOp.oC_PropertyOrLabelsExpression());
                return new CypherIn(valueExpression, arrExpression);
            }

            // array index - a[0] or a[0][1]
            if (listOp.oC_Expression().size() == 1) {
                CypherAstBase indexExpression = visitOC_Expression(listOp.oC_Expression().get(0));
                CypherArrayAccess arrayAccess = new CypherArrayAccess(valueExpression, indexExpression);
                children = children.subList(2, children.size());
                while (children.size() > 0) {
                    indexExpression = visitOC_Expression((CypherParser.OC_ExpressionContext) children.get(0));
                    arrayAccess = new CypherArrayAccess(arrayAccess, indexExpression);
                    children = children.subList(1, children.size());
                }
                return arrayAccess;
            }

            // array slice - v[1..3]
            if (listOp.oC_Expression().size() == 2) {
                CypherAstBase sliceFrom = visitOC_Expression(listOp.oC_Expression().get(0));
                CypherAstBase sliceTo = visitOC_Expression(listOp.oC_Expression().get(1));
                return new CypherArraySlice(valueExpression, sliceFrom, sliceTo);
            }
        }

        throw new VertexiumCypherNotImplemented("" + children.stream().map(ParseTree::getText).collect(Collectors.joining(", ")));
    }

    @Override
    public CypherAstBase visitOC_PropertyOrLabelsExpression(CypherParser.OC_PropertyOrLabelsExpressionContext ctx) {
        CypherParser.OC_AtomContext atom = ctx.oC_Atom();
        List<CypherParser.OC_PropertyLookupContext> propertyLookups = ctx.oC_PropertyLookup();
        CypherParser.OC_NodeLabelsContext nodeLabels = ctx.oC_NodeLabels();
        if ((propertyLookups == null || propertyLookups.size() == 0)
            && (nodeLabels == null || nodeLabels.oC_NodeLabel() == null || nodeLabels.oC_NodeLabel().size() == 0)) {
            if (ctx.children.size() != 1) {
                throw new VertexiumCypherSyntaxErrorException("invalid expression \"" + ctx.getText() + "\"");
            }
            return visitOC_Atom(atom);
        }
        return createLookup(atom, propertyLookups, nodeLabels);
    }

    private CypherLookup createLookup(
        CypherParser.OC_AtomContext atomCtx,
        List<CypherParser.OC_PropertyLookupContext> propertyLookups,
        CypherParser.OC_NodeLabelsContext nodeLabels
    ) {
        CypherAstBase atom = visitOC_Atom(atomCtx);
        if (propertyLookups.size() == 0 && (nodeLabels == null || nodeLabels.oC_NodeLabel().size() == 0)) {
            return new CypherLookup(atom, null, null);
        } else {
            String property = propertyLookups.stream()
                .map(pl -> visitOC_PropertyLookup(pl).getValue())
                .collect(Collectors.joining("."));
            if (property.length() == 0) {
                property = null;
            }
            List<CypherLabelName> labels;
            if (nodeLabels == null) {
                labels = new ArrayList<>();
            } else {
                labels = nodeLabels.oC_NodeLabel().stream()
                    .map(this::visitOC_NodeLabel)
                    .collect(Collectors.toList());
            }
            return new CypherLookup(atom, property, labels);
        }
    }

    @Override
    public CypherString visitOC_PropertyLookup(CypherParser.OC_PropertyLookupContext ctx) {
        return visitOC_PropertyKeyName(ctx.oC_PropertyKeyName());
    }

    @Override
    public CypherAstBase visitOC_Atom(CypherParser.OC_AtomContext ctx) {
        if (ctx.COUNT() != null) {
            return new CypherFunctionInvocation("count", false, new CypherMatchAll());
        }
        return super.visitOC_Atom(ctx);
    }

    @Override
    public CypherLiteral visitOC_Literal(CypherParser.OC_LiteralContext ctx) {
        if (ctx.StringLiteral() != null) {
            String text = ctx.StringLiteral().getText();
            return new CypherString(text.substring(1, text.length() - 1));
        }
        if (ctx.NULL() != null) {
            return new CypherNull();
        }
        return (CypherLiteral) super.visitOC_Literal(ctx);
    }

    @Override
    public CypherVariable visitOC_Variable(CypherParser.OC_VariableContext ctx) {
        if (ctx == null) {
            return null;
        }
        String name = visitOC_SymbolicName(ctx.oC_SymbolicName()).getValue();
        if (name == null) {
            return null;
        }
        return new CypherVariable(name);
    }

    public String visitVariableString(CypherParser.OC_VariableContext ctx) {
        CypherVariable v = visitOC_Variable(ctx);
        if (v == null) {
            return null;
        }
        return v.getName();
    }

    @Override
    public CypherString visitOC_SchemaName(CypherParser.OC_SchemaNameContext ctx) {
        if (ctx.oC_SymbolicName() != null) {
            return visitOC_SymbolicName(ctx.oC_SymbolicName());
        } else if (ctx.oC_ReservedWord() != null) {
            return visitOC_ReservedWord(ctx.oC_ReservedWord());
        } else {
            throw new VertexiumCypherSyntaxErrorException("Expected symbolic name or reserved word");
        }
    }

    @Override
    public CypherString visitOC_ReservedWord(CypherParser.OC_ReservedWordContext ctx) {
        return new CypherString(ctx.getText());
    }

    @Override
    public CypherString visitOC_SymbolicName(CypherParser.OC_SymbolicNameContext ctx) {
        if (ctx.EscapedSymbolicName() != null) {
            return visitOC_EscapedSymbolicName(ctx.EscapedSymbolicName());
        }
        return new CypherString(ctx.getText());
    }

    @Override
    public CypherListLiteral<CypherReturnItem> visitOC_ReturnItems(CypherParser.OC_ReturnItemsContext ctx) {
        if (ctx.children.get(0).getText().equals("*")) {
            return CypherListLiteral.of(new CypherReturnItem("*", new CypherAllLiteral(), null));
        }
        return ctx.oC_ReturnItem().stream()
            .map(this::visitOC_ReturnItem)
            .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherReturnItem visitOC_ReturnItem(CypherParser.OC_ReturnItemContext ctx) {
        return new CypherReturnItem(
            ctx.getText(),
            visitOC_Expression(ctx.oC_Expression()),
            visitVariableString(ctx.oC_Variable())
        );
    }

    @Override
    public CypherAstBase visitOC_PartialComparisonExpression(CypherParser.OC_PartialComparisonExpressionContext ctx) {
        throw new VertexiumCypherNotImplemented("PartialComparisonExpression");
    }

    @Override
    public CypherAstBase visitOC_ParenthesizedExpression(CypherParser.OC_ParenthesizedExpressionContext ctx) {
        return visitOC_Expression(ctx.oC_Expression());
    }

    @Override
    public CypherPatternComprehension visitOC_PatternComprehension(CypherParser.OC_PatternComprehensionContext ctx) {
        CypherVariable variable = ctx.oC_Variable() == null ? null : visitOC_Variable(ctx.oC_Variable());
        CypherRelationshipsPattern relationshipsPattern = visitOC_RelationshipsPattern(ctx.oC_RelationshipsPattern());
        List<CypherParser.OC_ExpressionContext> expressions = ctx.oC_Expression();
        CypherAstBase whereExpression = expressions.size() > 1 ? visitOC_Expression(expressions.get(0)) : null;
        CypherAstBase expression = visitOC_Expression(expressions.get(expressions.size() - 1));

        ArrayList<CypherElementPattern> patternPartPatterns = Lists.newArrayList(relationshipsPattern.getNodePattern());
        for (CypherElementPattern elementPattern : relationshipsPattern.getPatternElementChains()) {
            patternPartPatterns.add(elementPattern);
        }
        CypherPatternPart patternPart = new CypherPatternPart(variable == null ? null : variable.getName(), new CypherListLiteral<>(patternPartPatterns));
        CypherMatchClause matchClause = new CypherMatchClause(false, CypherListLiteral.of(patternPart), whereExpression);
        return new CypherPatternComprehension(matchClause, expression);
    }

    @Override
    public CypherLimit visitOC_Limit(CypherParser.OC_LimitContext ctx) {
        String expressionText = ctx.oC_Expression().getText();
        Integer i = tryParseInteger(expressionText);
        if (i != null && i < 0) {
            throw new VertexiumCypherSyntaxErrorException("NegativeIntegerArgument: limit should only have positive arguments: " + expressionText);
        }

        CypherAstBase expression = visitOC_Expression(ctx.oC_Expression());
        return new CypherLimit(expression);
    }

    private Integer tryParseInteger(String expressionText) {
        try {
            return Integer.parseInt(expressionText);
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public CypherBoolean visitOC_BooleanLiteral(CypherParser.OC_BooleanLiteralContext ctx) {
        if (ctx.TRUE() != null) {
            return new CypherBoolean(true);
        }
        if (ctx.FALSE() != null) {
            return new CypherBoolean(false);
        }
        throw new VertexiumException("unexpected boolean: " + ctx.getText());
    }

    @Override
    public CypherOrderBy visitOC_Order(CypherParser.OC_OrderContext ctx) {
        List<CypherSortItem> sortItems = ctx.oC_SortItem().stream()
            .map(this::visitOC_SortItem)
            .collect(Collectors.toList());
        return new CypherOrderBy(sortItems);
    }

    @Override
    public CypherIdInColl visitOC_IdInColl(CypherParser.OC_IdInCollContext ctx) {
        CypherVariable variable = visitOC_Variable(ctx.oC_Variable());
        CypherAstBase expression = visitOC_Expression(ctx.oC_Expression());
        return new CypherIdInColl(variable, expression);
    }

    @Override
    public CypherRelTypeName visitOC_RelTypeName(CypherParser.OC_RelTypeNameContext ctx) {
        return new CypherRelTypeName(visitOC_SchemaName(ctx.oC_SchemaName()).getValue());
    }

    @Override
    public CypherDouble visitOC_DoubleLiteral(CypherParser.OC_DoubleLiteralContext ctx) {
        return new CypherDouble(Double.parseDouble(ctx.getText()));
    }

    @Override
    public CypherAstBase visitOC_Dash(CypherParser.OC_DashContext ctx) {
        throw new VertexiumCypherNotImplemented("Dash");
    }

    @Override
    public CypherLabelName visitOC_NodeLabel(CypherParser.OC_NodeLabelContext ctx) {
        return visitOC_LabelName(ctx.oC_LabelName());
    }

    @Override
    public CypherAstBase visitOC_RightArrowHead(CypherParser.OC_RightArrowHeadContext ctx) {
        throw new VertexiumCypherNotImplemented("RightArrowHead");
    }

    @Override
    public CypherAstBase visitOC_PropertyExpression(CypherParser.OC_PropertyExpressionContext ctx) {
        if (ctx.oC_PropertyLookup() != null) {
            return createLookup(ctx.oC_Atom(), ctx.oC_PropertyLookup(), null);
        }
        return visitOC_Atom(ctx.oC_Atom());
    }

    @Override
    public CypherRemoveItem visitOC_RemoveItem(CypherParser.OC_RemoveItemContext ctx) {
        if (ctx.oC_PropertyExpression() != null) {
            return new CypherRemovePropertyExpressionItem(visitOC_PropertyExpression(ctx.oC_PropertyExpression()));
        } else {
            return new CypherRemoveLabelItem(
                visitOC_Variable(ctx.oC_Variable()),
                visitOC_NodeLabels(ctx.oC_NodeLabels())
            );
        }
    }

    @Override
    public CypherListLiteral<CypherAstBase> visitOC_ListLiteral(CypherParser.OC_ListLiteralContext ctx) {
        return visitExpressions(ctx.oC_Expression());
    }

    @Override
    public CypherSkip visitOC_Skip(CypherParser.OC_SkipContext ctx) {
        CypherAstBase expression = visitOC_Expression(ctx.oC_Expression());
        return new CypherSkip(expression);
    }

    @Override
    public CypherAstBase visitOC_LeftArrowHead(CypherParser.OC_LeftArrowHeadContext ctx) {
        throw new VertexiumCypherNotImplemented("LeftArrowHead");
    }

    @Override
    public CypherDeleteClause visitOC_Delete(CypherParser.OC_DeleteContext ctx) {
        boolean detach = ctx.DETACH() != null;
        CypherListLiteral<CypherAstBase> expressions = visitExpressions(ctx.oC_Expression());
        return new CypherDeleteClause(expressions, detach);
    }

    @Override
    public CypherRemoveClause visitOC_Remove(CypherParser.OC_RemoveContext ctx) {
        List<CypherRemoveItem> removeItems = ctx.oC_RemoveItem().stream()
            .map(this::visitOC_RemoveItem)
            .collect(Collectors.toList());
        return new CypherRemoveClause(removeItems);
    }

    @Override
    public CypherAstBase visitOC_FunctionInvocation(CypherParser.OC_FunctionInvocationContext ctx) {
        String functionName = visitOC_FunctionName(ctx.oC_FunctionName()).getValue();
        CypherFunction fn = compilerContext.getFunction(functionName);
        if (fn == null) {
            throw new VertexiumCypherSyntaxErrorException("UnknownFunction: Could not find function with name \"" + functionName + "\"");
        }
        boolean distinct = ctx.DISTINCT() != null;
        CypherListLiteral<CypherAstBase> argumentsList = visitExpressions(ctx.oC_Expression());
        CypherAstBase[] arguments = argumentsList.toArray(new CypherAstBase[argumentsList.size()]);
        return new CypherFunctionInvocation(functionName, distinct, arguments);
    }

    @Override
    public CypherAstBase visitOC_ListComprehension(CypherParser.OC_ListComprehensionContext ctx) {
        CypherFilterExpression filterExpression = visitOC_FilterExpression(ctx.oC_FilterExpression());
        CypherAstBase expression = ctx.oC_Expression() == null ? null : visitOC_Expression(ctx.oC_Expression());
        return new CypherListComprehension(filterExpression, expression);
    }

    @Override
    public CypherStatement visitOC_Cypher(CypherParser.OC_CypherContext ctx) {
        return visitOC_Statement(ctx.oC_Statement());
    }

    @Override
    public CypherAstBase visitOC_Parameter(CypherParser.OC_ParameterContext ctx) {
        if (ctx.oC_SymbolicName() != null) {
            String parameterName = visitOC_SymbolicName(ctx.oC_SymbolicName()).getValue();
            return new CypherNameParameter(parameterName);
        }
        if (ctx.DecimalInteger() != null) {
            return new CypherIndexedParameter(Integer.parseInt(ctx.DecimalInteger().getText()));
        }
        throw new VertexiumCypherNotImplemented("Parameter");
    }

    @Override
    public CypherMergeAction visitOC_MergeAction(CypherParser.OC_MergeActionContext ctx) {
        CypherSetClause set = visitOC_Set(ctx.oC_Set());
        if (ctx.CREATE() != null) {
            return new CypherMergeActionCreate(set);
        } else if (ctx.MATCH() != null) {
            return new CypherMergeActionMatch(set);
        } else {
            throw new VertexiumCypherSyntaxErrorException("Expected ON CREATE or ON MATCH");
        }
    }

    @Override
    public CypherSortItem visitOC_SortItem(CypherParser.OC_SortItemContext ctx) {
        CypherAstBase expr = visitOC_Expression(ctx.oC_Expression());
        CypherSortItem.Direction direction;
        if (ctx.DESC() != null || ctx.DESCENDING() != null) {
            direction = CypherSortItem.Direction.DESCENDING;
        } else {
            direction = CypherSortItem.Direction.ASCENDING;
        }
        return new CypherSortItem(expr, direction, ctx.oC_Expression().getText());
    }

    @Override
    public CypherSetItem visitOC_SetItem(CypherParser.OC_SetItemContext ctx) {
        if (ctx.oC_PropertyExpression() != null) {
            CypherAstBase lookup = visitOC_PropertyExpression(ctx.oC_PropertyExpression());
            if (!(lookup instanceof CypherLookup)) {
                throw new VertexiumException("expected " + CypherLookup.class.getName() + " found " + lookup.getClass().getName());
            }
            return new CypherSetProperty(
                (CypherLookup) lookup,
                visitOC_Expression(ctx.oC_Expression())
            );
        }

        if (ctx.oC_NodeLabels() != null) {
            return new CypherSetNodeLabels(
                visitOC_Variable(ctx.oC_Variable()),
                visitOC_NodeLabels(ctx.oC_NodeLabels())
            );
        }

        CypherSetItem.Op op = getSetItemOp(ctx);
        return new CypherSetVariable(
            visitOC_Variable(ctx.oC_Variable()),
            op,
            visitOC_Expression(ctx.oC_Expression())
        );
    }

    private CypherSetItem.Op getSetItemOp(CypherParser.OC_SetItemContext ctx) {
        for (ParseTree child : ctx.children) {
            if (child instanceof TerminalNode) {
                String text = child.getText();
                if (text.equals("+=")) {
                    return CypherSetItem.Op.PLUS_EQUAL;
                } else if (text.equals("=")) {
                    return CypherSetItem.Op.EQUAL;
                }
            }
        }
        throw new VertexiumException("Could not find set item op: " + ctx.getText());
    }

    @Override
    public CypherSetClause visitOC_Set(CypherParser.OC_SetContext ctx) {
        return new CypherSetClause(ctx.oC_SetItem().stream().map(this::visitOC_SetItem).collect(Collectors.toList()));
    }

    @Override
    public CypherString visitOC_FunctionName(CypherParser.OC_FunctionNameContext ctx) {
        String functionName = "";
        if (ctx.oC_Namespace() != null) {
            functionName += ctx.oC_Namespace().getText();
        }
        if (ctx.oC_SymbolicName() != null) {
            functionName += visitOC_SymbolicName(ctx.oC_SymbolicName()).getValue();
        }
        if (functionName.trim().length() == 0) {
            functionName = ctx.getText();
        }
        return new CypherString(functionName);
    }

    private CypherString visitOC_EscapedSymbolicName(TerminalNode escapedSymbolicName) {
        String text = escapedSymbolicName.getText();
        text = text.substring(1, text.length() - 1);
        return new CypherString(text);
    }

    private CypherString visitUnescapedSymbolicName(TerminalNode unescapedSymbolicName) {
        return new CypherString(unescapedSymbolicName.getText());
    }

    @Override
    public CypherRelationshipsPattern visitOC_RelationshipsPattern(CypherParser.OC_RelationshipsPatternContext ctx) {
        CypherNodePattern nodePattern = visitOC_NodePattern(ctx.oC_NodePattern());
        List<CypherElementPattern> patternElementChains = visitPatternElementChainList(nodePattern, ctx.oC_PatternElementChain());
        return new CypherRelationshipsPattern(nodePattern, patternElementChains);
    }

    private CypherAstBase visitUnions(CypherQuery left, List<CypherParser.OC_UnionContext> unions) {
        if (unions.size() == 0) {
            return left;
        }
        CypherParser.OC_UnionContext firstUnion = unions.get(0);
        boolean all = firstUnion.ALL() != null;
        CypherQuery right = visitSingleQuery(firstUnion.oC_SingleQuery());
        return new CypherUnion(left, visitUnions(right, unions.subList(1, unions.size())), all);
    }

    @Override
    public CypherUnion visitOC_Union(CypherParser.OC_UnionContext ctx) {
        throw new VertexiumCypherNotImplemented("Union");
    }

    @Override
    public CypherAstBase visitOC_RelationshipDetail(CypherParser.OC_RelationshipDetailContext ctx) {
        throw new VertexiumCypherNotImplemented("RelationshipDetail");
    }

    @Override
    public CypherRangeLiteral visitOC_RangeLiteral(CypherParser.OC_RangeLiteralContext ctx) {
        Integer from = null;
        Integer to = null;
        boolean seenDotDot = false;
        for (ParseTree child : ctx.children) {
            if (child instanceof CypherParser.OC_IntegerLiteralContext) {
                int i = visitOC_IntegerLiteral((CypherParser.OC_IntegerLiteralContext) child).getIntValue();
                if (seenDotDot) {
                    to = i;
                } else {
                    from = i;
                }
                continue;
            }
            String text = child.getText();
            if (text.equals("*")) {
                continue;
            }
            if (text.equals("..")) {
                seenDotDot = true;
                continue;
            }
        }
        if (!seenDotDot) {
            to = from;
        }
        return new CypherRangeLiteral(from, to);
    }

    @Override
    public CypherFilterExpression visitOC_FilterExpression(CypherParser.OC_FilterExpressionContext ctx) {
        CypherIdInColl idInCol = visitOC_IdInColl(ctx.oC_IdInColl());
        CypherAstBase where = ctx.oC_Where() == null ? null : visitOC_Where(ctx.oC_Where());
        return new CypherFilterExpression(idInCol, where);
    }

    @Override
    public CypherInteger visitOC_IntegerLiteral(CypherParser.OC_IntegerLiteralContext ctx) {
        try {
            return new CypherInteger(Long.decode(ctx.getText()));
        } catch (NumberFormatException ex) {
            throw new VertexiumException("could not parse \"" + ctx.getText() + "\" into integer");
        }
    }

    @Override
    public CypherListLiteral<CypherRelTypeName> visitOC_RelationshipTypes(CypherParser.OC_RelationshipTypesContext ctx) {
        return ctx.oC_RelTypeName().stream()
            .map(this::visitOC_RelTypeName)
            .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherLiteral visitOC_NumberLiteral(CypherParser.OC_NumberLiteralContext ctx) {
        return (CypherLiteral) super.visitOC_NumberLiteral(ctx);
    }

    @Override
    public CypherAstBase visitErrorNode(ErrorNode node) {
        throw new VertexiumException(String.format(
            "Could not parse: invalid value \"%s\" (line: %d, pos: %d)",
            node.getText(),
            node.getSymbol().getLine(),
            node.getSymbol().getCharPositionInLine()
        ));
    }
}

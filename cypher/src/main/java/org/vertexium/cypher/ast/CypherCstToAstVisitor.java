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
    public CypherStatement visitStatement(CypherParser.StatementContext ctx) {
        return new CypherStatement(visitQuery(ctx.query()));
    }

    @Override
    public CypherAstBase visitQuery(CypherParser.QueryContext ctx) {
        return visitRegularQuery(ctx.regularQuery());
    }

    @Override
    public CypherAstBase visitRegularQuery(CypherParser.RegularQueryContext ctx) {
        CypherQuery left = visitSingleQuery(ctx.singleQuery());
        if (ctx.union().size() > 0) {
            return visitUnions(left, ctx.union());
        }
        return left;
    }

    @Override
    public CypherQuery visitSingleQuery(CypherParser.SingleQueryContext ctx) {
        return new CypherQuery(
                ctx.clause().stream()
                        .map(this::visitClause)
                        .collect(StreamUtils.toImmutableList())
        );
    }

    @Override
    public CypherClause visitClause(CypherParser.ClauseContext ctx) {
        Object o = super.visitClause(ctx);
        if (!(o instanceof CypherClause)) {
            throw new VertexiumException("clause not supported: " + ctx.getText());
        }
        return (CypherClause) o;
    }

    @Override
    public CypherCreateClause visitCreate(CypherParser.CreateContext ctx) {
        ImmutableList<CypherPatternPart> patternParts = ctx.pattern().patternPart().stream()
                .map(this::visitPatternPart)
                .collect(StreamUtils.toImmutableList());
        return new CypherCreateClause(patternParts);
    }

    @Override
    public CypherReturnClause visitReturnClause(CypherParser.ReturnClauseContext ctx) {
        boolean distinct = ctx.DISTINCT() != null;
        return new CypherReturnClause(distinct, visitReturnBody(ctx.returnBody()));
    }

    @Override
    public CypherReturnBody visitReturnBody(CypherParser.ReturnBodyContext ctx) {
        CypherParser.OrderContext order = ctx.order();
        CypherParser.LimitContext limit = ctx.limit();
        CypherParser.SkipContext skip = ctx.skip();
        return new CypherReturnBody(
                visitReturnItems(ctx.returnItems()),
                (order == null) ? null : visitOrder(order),
                (limit == null) ? null : visitLimit(limit),
                (skip == null) ? null : visitSkip(skip)
        );
    }

    @Override
    public CypherMatchClause visitMatch(CypherParser.MatchContext ctx) {
        boolean optional = ctx.OPTIONAL() != null;
        CypherListLiteral<CypherPatternPart> patternParts = visitPattern(ctx.pattern());
        CypherAstBase whereExpression = visitWhere(ctx.where());
        return new CypherMatchClause(optional, patternParts, whereExpression);
    }

    @Override
    public CypherListLiteral<CypherPatternPart> visitPattern(CypherParser.PatternContext ctx) {
        return ctx.patternPart().stream()
                .map(this::visitPatternPart)
                .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherPatternPart visitPatternPart(CypherParser.PatternPartContext ctx) {
        String name = visitVariableString(ctx.variable());
        CypherListLiteral<CypherElementPattern> elementPatterns = visitAnonymousPatternPart(ctx.anonymousPatternPart());
        return new CypherPatternPart(name, elementPatterns);
    }

    @Override
    public CypherListLiteral<CypherElementPattern> visitAnonymousPatternPart(CypherParser.AnonymousPatternPartContext ctx) {
        return visitPatternElement(ctx.patternElement());
    }

    @Override
    public CypherListLiteral<CypherElementPattern> visitPatternElement(CypherParser.PatternElementContext ctx) {
        // unwind parenthesis
        if (ctx.patternElement() != null) {
            return visitPatternElement(ctx.patternElement());
        }

        List<CypherElementPattern> list = new ArrayList<>();
        CypherNodePattern nodePattern = visitNodePattern(ctx.nodePattern());
        list.add(nodePattern);
        list.addAll(visitPatternElementChainList(nodePattern, ctx.patternElementChain()));
        return new CypherListLiteral<>(list);
    }

    private List<CypherElementPattern> visitPatternElementChainList(
            CypherNodePattern previousNodePattern,
            List<CypherParser.PatternElementChainContext> patternElementChainList
    ) {
        List<CypherElementPattern> list = new ArrayList<>();
        for (CypherParser.PatternElementChainContext chainContext : patternElementChainList) {
            CypherRelationshipPattern relationshipPattern = visitRelationshipPattern(chainContext.relationshipPattern());
            relationshipPattern.setPreviousNodePattern(previousNodePattern);
            list.add(relationshipPattern);

            CypherNodePattern nodePattern = visitNodePattern(chainContext.nodePattern());
            relationshipPattern.setNextNodePattern(nodePattern);
            list.add(nodePattern);

            previousNodePattern = nodePattern;
        }
        return list;
    }

    @Override
    public CypherNodePattern visitNodePattern(CypherParser.NodePatternContext ctx) {
        return new CypherNodePattern(
                visitVariableString(ctx.variable()),
                visitProperties(ctx.properties()),
                visitNodeLabels(ctx.nodeLabels())
        );
    }

    @Override
    public CypherRelationshipPattern visitRelationshipPattern(CypherParser.RelationshipPatternContext ctx) {
        CypherParser.RelationshipDetailContext relationshipDetail = ctx.relationshipDetail();
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
            if (relationshipDetail.rangeLiteral() != null) {
                range = visitRangeLiteral(relationshipDetail.rangeLiteral());
            } else {
                range = null;
            }
            name = visitVariableString(relationshipDetail.variable());
            if (relationshipDetail.relationshipTypes() == null) {
                relTypeNames = null;
            } else {
                relTypeNames = visitRelationshipTypes(relationshipDetail.relationshipTypes());
            }
            properties = visitProperties(relationshipDetail.properties());
        }

        CypherDirection direction = getDirectionFromRelationshipPattern(ctx);
        return new CypherRelationshipPattern(name, relTypeNames, properties, range, direction);
    }

    private static CypherDirection getDirectionFromRelationshipPattern(CypherParser.RelationshipPatternContext relationshipPatternContext) {
        if (relationshipPatternContext.leftArrowHead() != null && relationshipPatternContext.rightArrowHead() != null) {
            return CypherDirection.BOTH;
        }
        if (relationshipPatternContext.leftArrowHead() != null) {
            return CypherDirection.IN;
        }
        if (relationshipPatternContext.rightArrowHead() != null) {
            return CypherDirection.OUT;
        }
        return CypherDirection.UNSPECIFIED;
    }

    @Override
    public CypherMapLiteral<String, CypherAstBase> visitProperties(CypherParser.PropertiesContext ctx) {
        if (ctx == null) {
            return null;
        }
        //noinspection unchecked
        return (CypherMapLiteral<String, CypherAstBase>) super.visitProperties(ctx);
    }

    @Override
    public CypherMapLiteral<String, CypherAstBase> visitMapLiteral(CypherParser.MapLiteralContext ctx) {
        List<CypherParser.PropertyKeyNameContext> keys = ctx.propertyKeyName();
        List<CypherParser.ExpressionContext> values = ctx.expression();
        Map<String, CypherAstBase> result = new HashMap<>();
        for (int i = 0, keysSize = keys.size(); i < keysSize; i++) {
            String key = visitPropertyKeyName(keys.get(i)).getValue();
            CypherAstBase value = visitExpression(values.get(i));
            result.put(key, value);
        }
        return new CypherMapLiteral<>(result);
    }

    @Override
    public CypherString visitPropertyKeyName(CypherParser.PropertyKeyNameContext ctx) {
        return visitSymbolicName(ctx.symbolicName());
    }

    @Override
    public CypherListLiteral<CypherLabelName> visitNodeLabels(CypherParser.NodeLabelsContext ctx) {
        if (ctx == null) {
            return new CypherListLiteral<>();
        }
        return ctx.nodeLabel().stream()
                .map(nl -> visitLabelName(nl.labelName()))
                .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherLabelName visitLabelName(CypherParser.LabelNameContext ctx) {
        return new CypherLabelName(visitSymbolicName(ctx.symbolicName()).getValue());
    }

    @Override
    public CypherAstBase visitPatternElementChain(CypherParser.PatternElementChainContext ctx) {
        throw new VertexiumException("should not be called, see visitPatternElementChainList");
    }

    @Override
    public CypherUnwindClause visitUnwind(CypherParser.UnwindContext ctx) {
        String name = visitVariableString(ctx.variable());
        CypherAstBase expression = visitExpression(ctx.expression());
        return new CypherUnwindClause(name, expression);
    }

    @Override
    public CypherWithClause visitWith(CypherParser.WithContext ctx) {
        boolean distinct = ctx.DISTINCT() != null;
        CypherReturnBody returnBody = visitReturnBody(ctx.returnBody());
        CypherAstBase where = visitWhere(ctx.where());
        return new CypherWithClause(distinct, returnBody, where);
    }

    @Override
    public CypherMergeClause visitMerge(CypherParser.MergeContext ctx) {
        CypherPatternPart patternPart = visitPatternPart(ctx.patternPart());
        List<CypherMergeAction> mergeActions = ctx.mergeAction().stream()
                .map(this::visitMergeAction)
                .collect(Collectors.toList());
        return new CypherMergeClause(
                patternPart,
                mergeActions
        );
    }

    @Override
    public CypherAstBase visitWhere(CypherParser.WhereContext ctx) {
        if (ctx == null) {
            return null;
        }
        return visitExpression(ctx.expression());
    }

    public CypherListLiteral<CypherAstBase> visitExpressions(Iterable<CypherParser.ExpressionContext> expressionContexts) {
        return stream(expressionContexts)
                .map(this::visitExpression)
                .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherAstBase visitExpression(CypherParser.ExpressionContext ctx) {
        return visitExpression12(ctx.expression12());
    }

    // OR
    @Override
    public CypherAstBase visitExpression12(CypherParser.Expression12Context ctx) {
        List<CypherParser.Expression11Context> children = ctx.expression11();
        if (children.size() == 1) {
            return visitExpression11(children.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitExpression11);
    }

    // XOR
    @Override
    public CypherAstBase visitExpression11(CypherParser.Expression11Context ctx) {
        List<CypherParser.Expression10Context> children = ctx.expression10();
        if (children.size() == 1) {
            return visitExpression10(children.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitExpression10);
    }

    // AND
    @Override
    public CypherAstBase visitExpression10(CypherParser.Expression10Context ctx) {
        List<CypherParser.Expression9Context> children = ctx.expression9();
        if (children.size() == 1) {
            return visitExpression9(children.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitExpression9);
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

    // NOT
    @Override
    public CypherAstBase visitExpression9(CypherParser.Expression9Context ctx) {
        if (ctx.NOT().size() % 2 == 0) {
            return visitExpression8(ctx.expression8());
        } else {
            return new CypherUnaryExpression(CypherUnaryExpression.Op.NOT, visitExpression8(ctx.expression8()));
        }
    }

    // comparison
    @Override
    public CypherAstBase visitExpression8(CypherParser.Expression8Context ctx) {
        List<CypherParser.PartialComparisonExpressionContext> partialComparisonExpressions = ctx.partialComparisonExpression();
        if (partialComparisonExpressions.size() == 0) {
            return visitExpression7(ctx.expression7());
        }

        CypherAstBase left = visitExpression7(ctx.expression7());
        String op = partialComparisonExpressions.get(0).children.get(0).getText();
        CypherAstBase right = visitExpression7(partialComparisonExpressions.get(0).expression7());
        return new CypherBinaryExpression(
                new CypherComparisonExpression(left, op, right),
                CypherBinaryExpression.Op.AND,
                visitPartialComparisonExpression(right, 1, partialComparisonExpressions)
        );
    }

    private CypherExpression visitPartialComparisonExpression(
            CypherAstBase left,
            int partialComparisonExpressionIndex,
            List<CypherParser.PartialComparisonExpressionContext> partialComparisonExpressions
    ) {
        if (partialComparisonExpressionIndex >= partialComparisonExpressions.size()) {
            return new CypherTrueExpression();
        }
        String op = partialComparisonExpressions.get(partialComparisonExpressionIndex).children.get(0).getText();
        CypherAstBase right = visitExpression7(partialComparisonExpressions.get(partialComparisonExpressionIndex).expression7());
        CypherComparisonExpression binLeft = new CypherComparisonExpression(left, op, right);
        CypherExpression binRight = visitPartialComparisonExpression(right, partialComparisonExpressionIndex + 1, partialComparisonExpressions);
        if (binRight instanceof CypherTrueExpression) {
            return binLeft;
        }
        return new CypherBinaryExpression(binLeft, CypherBinaryExpression.Op.AND, binRight);
    }

    // + -
    @Override
    public CypherAstBase visitExpression7(CypherParser.Expression7Context ctx) {
        List<CypherParser.Expression6Context> expression6s = ctx.expression6();
        if (expression6s.size() == 1) {
            return visitExpression6(expression6s.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitExpression6);
    }

    // * / %
    @Override
    public CypherAstBase visitExpression6(CypherParser.Expression6Context ctx) {
        List<CypherParser.Expression5Context> expression5s = ctx.expression5();
        if (expression5s.size() == 1) {
            return visitExpression5(expression5s.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitExpression5);
    }

    // ^
    @Override
    public CypherAstBase visitExpression5(CypherParser.Expression5Context ctx) {
        List<CypherParser.Expression4Context> expression4s = ctx.expression4();
        if (expression4s.size() == 1) {
            return visitExpression4(expression4s.get(0));
        }
        return toBinaryExpressions(ctx.children, this::visitExpression4);
    }

    // + - prefix
    @Override
    public CypherAstBase visitExpression4(CypherParser.Expression4Context ctx) {
        int neg = 0;
        for (ParseTree child : ctx.children) {
            if (child instanceof TerminalNode && child.getText().equals("-")) {
                neg++;
            }
        }
        CypherAstBase expr = visitExpression3(ctx.expression3());
        if (neg % 2 == 1) {
            return new CypherNegateExpression(expr);
        } else {
            return expr;
        }
    }

    @Override
    public CypherAstBase visitExpression3(CypherParser.Expression3Context ctx) {
        if (ctx.children.size() == 1) {
            return visitExpression2(ctx.expression2(0));
        }
        return visitExpression3(filterSpaces(ctx.children).collect(Collectors.toList()));
    }

    private Stream<ParseTree> filterSpaces(List<ParseTree> items) {
        return items.stream()
                .filter(item -> item.getText().trim().length() > 0);
    }

    private CypherAstBase visitExpression3(List<ParseTree> children) {
        // array slice - v[1..3]
        if (children.size() == 6
                && children.get(1).getText().equals("[")
                && children.get(3).getText().equals("..")
                && children.get(5).getText().equals("]")) {
            CypherAstBase arrayExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            CypherAstBase sliceFrom = visitExpression((CypherParser.ExpressionContext) children.get(2));
            CypherAstBase sliceTo = visitExpression((CypherParser.ExpressionContext) children.get(4));
            return new CypherArraySlice(arrayExpression, sliceFrom, sliceTo);
        }

        // item in list - 'a' IN [ 1, 2, 3 ]
        else if (children.size() > 2
                && children.get(1).getText().equalsIgnoreCase("IN")) {
            CypherAstBase valueExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            List<ParseTree> remainingChildren = children.stream().skip(2).collect(Collectors.toList());
            CypherAstBase arrExpression;
            if (remainingChildren.size() == 1) {
                arrExpression = visitExpression2((CypherParser.Expression2Context) remainingChildren.get(0));
            } else {
                arrExpression = visitExpression3(remainingChildren);
            }
            return new CypherIn(valueExpression, arrExpression);
        }

        // is null - a IS NULL
        else if (children.size() == 3
                && children.get(1).getText().equalsIgnoreCase("IS")
                && children.get(2).getText().equalsIgnoreCase("NULL")) {
            CypherAstBase valueExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            return new CypherIsNull(valueExpression);
        }

        // is not null - a IS NOT NULL
        else if (children.size() == 4
                && children.get(1).getText().equalsIgnoreCase("IS")
                && children.get(2).getText().equalsIgnoreCase("NOT")
                && children.get(3).getText().equalsIgnoreCase("NULL")) {
            CypherAstBase valueExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            return new CypherIsNotNull(valueExpression);
        }

        // starts with - 'abc' STARTS WITH 'a'
        else if (children.size() == 4
                && children.get(1).getText().equalsIgnoreCase("STARTS")
                && children.get(2).getText().equalsIgnoreCase("WITH")) {
            CypherAstBase valueExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            CypherAstBase stringExpression = visitExpression2((CypherParser.Expression2Context) children.get(3));
            return new CypherStringMatch(valueExpression, stringExpression, CypherStringMatch.Op.STARTS_WITH);
        }

        // ends with - 'abc' ENDS WITH 'a'
        else if (children.size() == 4
                && children.get(1).getText().equalsIgnoreCase("ENDS")
                && children.get(2).getText().equalsIgnoreCase("WITH")) {
            CypherAstBase valueExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            CypherAstBase stringExpression = visitExpression2((CypherParser.Expression2Context) children.get(3));
            return new CypherStringMatch(valueExpression, stringExpression, CypherStringMatch.Op.ENDS_WITH);
        }

        // contains - 'abc' CONTAINS 'a'
        else if (children.size() == 3
                && children.get(1).getText().equalsIgnoreCase("CONTAINS")) {
            CypherAstBase valueExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            CypherAstBase stringExpression = visitExpression2((CypherParser.Expression2Context) children.get(2));
            return new CypherStringMatch(valueExpression, stringExpression, CypherStringMatch.Op.CONTAINS);
        }

        // array index - a[0] or a[0][1]
        else if (children.size() >= 4 && children.get(1).getText().equals("[") && children.get(3).getText().equals("]")) {
            CypherAstBase arrayExpression = visitExpression2((CypherParser.Expression2Context) children.get(0));
            CypherAstBase indexExpression = visitExpression((CypherParser.ExpressionContext) children.get(2));
            CypherArrayAccess arrayAccess = new CypherArrayAccess(arrayExpression, indexExpression);
            children = children.subList(4, children.size());
            while (children.size() > 0) {
                indexExpression = visitExpression((CypherParser.ExpressionContext) children.get(1));
                arrayAccess = new CypherArrayAccess(arrayAccess, indexExpression);
                children = children.subList(3, children.size());
            }
            return arrayAccess;
        }

        throw new VertexiumCypherNotImplemented("" + children.stream().map(ParseTree::getText).collect(Collectors.joining(", ")));
    }

    @Override
    public CypherAstBase visitExpression2(CypherParser.Expression2Context ctx) {
        CypherParser.AtomContext atom = ctx.atom();
        List<CypherParser.PropertyLookupContext> propertyLookups = ctx.propertyLookup();
        List<CypherParser.NodeLabelsContext> nodeLabels = ctx.nodeLabels();
        if ((propertyLookups == null || propertyLookups.size() == 0) && (nodeLabels == null || nodeLabels.size() == 0)) {
            if (ctx.children.size() != 1) {
                throw new VertexiumCypherSyntaxErrorException("invalid expression \"" + ctx.getText() + "\"");
            }
            return visitAtom(atom);
        }
        return createLookup(atom, propertyLookups, nodeLabels);
    }

    private CypherLookup createLookup(
            CypherParser.AtomContext atomCtx,
            List<CypherParser.PropertyLookupContext> propertyLookups,
            List<CypherParser.NodeLabelsContext> nodeLabels
    ) {
        CypherAstBase atom = visitAtom(atomCtx);
        if (propertyLookups.size() == 0 && nodeLabels.size() == 0) {
            return new CypherLookup(atom, null, null);
        } else {
            String property = propertyLookups.stream()
                    .map(pl -> visitPropertyLookup(pl).getValue())
                    .collect(Collectors.joining("."));
            if (property.length() == 0) {
                property = null;
            }
            List<CypherLabelName> labels;
            if (nodeLabels == null) {
                labels = null;
            } else {
                labels = nodeLabels.stream()
                        .flatMap(l -> visitNodeLabels(l).getValue().stream())
                        .collect(Collectors.toList());
            }
            return new CypherLookup(atom, property, labels);
        }
    }

    @Override
    public CypherString visitPropertyLookup(CypherParser.PropertyLookupContext ctx) {
        return visitPropertyKeyName(ctx.propertyKeyName());
    }

    @Override
    public CypherAstBase visitAtom(CypherParser.AtomContext ctx) {
        if (ctx.COUNT() != null) {
            return new CypherFunctionInvocation("count", false, new CypherMatchAll());
        }
        return super.visitAtom(ctx);
    }

    @Override
    public CypherLiteral visitLiteral(CypherParser.LiteralContext ctx) {
        if (ctx.StringLiteral() != null) {
            String text = ctx.StringLiteral().getText();
            return new CypherString(text.substring(1, text.length() - 1));
        }
        return (CypherLiteral) super.visitLiteral(ctx);
    }

    @Override
    public CypherVariable visitVariable(CypherParser.VariableContext ctx) {
        if (ctx == null) {
            return null;
        }
        String name = visitSymbolicName(ctx.symbolicName()).getValue();
        if (name == null) {
            return null;
        }
        return new CypherVariable(name);
    }

    public String visitVariableString(CypherParser.VariableContext ctx) {
        CypherVariable v = visitVariable(ctx);
        if (v == null) {
            return null;
        }
        return v.getName();
    }

    @Override
    public CypherString visitSymbolicName(CypherParser.SymbolicNameContext ctx) {
        if (ctx.EscapedSymbolicName() != null) {
            return visitEscapedSymbolicName(ctx.EscapedSymbolicName());
        }
        return new CypherString(ctx.getText());
    }

    @Override
    public CypherListLiteral<CypherReturnItem> visitReturnItems(CypherParser.ReturnItemsContext ctx) {
        if (ctx.children.get(0).getText().equals("*")) {
            return CypherListLiteral.of(new CypherReturnItem("*", new CypherAllLiteral(), null));
        }
        return ctx.returnItem().stream()
                .map(this::visitReturnItem)
                .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherReturnItem visitReturnItem(CypherParser.ReturnItemContext ctx) {
        return new CypherReturnItem(
                ctx.getText(),
                visitExpression(ctx.expression()),
                visitVariableString(ctx.variable())
        );
    }

    @Override
    public CypherAstBase visitPartialComparisonExpression(CypherParser.PartialComparisonExpressionContext ctx) {
        throw new VertexiumCypherNotImplemented("PartialComparisonExpression");
    }

    @Override
    public CypherAstBase visitParenthesizedExpression(CypherParser.ParenthesizedExpressionContext ctx) {
        return visitExpression(ctx.expression());
    }

    @Override
    public CypherPatternComprehension visitPatternComprehension(CypherParser.PatternComprehensionContext ctx) {
        CypherVariable variable = ctx.variable() == null ? null : visitVariable(ctx.variable());
        CypherRelationshipsPattern relationshipsPattern = visitRelationshipsPattern(ctx.relationshipsPattern());
        List<CypherParser.ExpressionContext> expressions = ctx.expression();
        CypherAstBase whereExpression = expressions.size() > 1 ? visitExpression(expressions.get(0)) : null;
        CypherAstBase expression = visitExpression(expressions.get(expressions.size() - 1));

        ArrayList<CypherElementPattern> patternPartPatterns = Lists.newArrayList(relationshipsPattern.getNodePattern());
        for (CypherElementPattern elementPattern : relationshipsPattern.getPatternElementChains()) {
            patternPartPatterns.add(elementPattern);
        }
        CypherPatternPart patternPart = new CypherPatternPart(variable == null ? null : variable.getName(), new CypherListLiteral<>(patternPartPatterns));
        CypherMatchClause matchClause = new CypherMatchClause(false, CypherListLiteral.of(patternPart), whereExpression);
        return new CypherPatternComprehension(matchClause, expression);
    }

    @Override
    public CypherLimit visitLimit(CypherParser.LimitContext ctx) {
        String expressionText = ctx.expression().getText();
        Integer i = tryParseInteger(expressionText);
        if (i != null && i < 0) {
            throw new VertexiumCypherSyntaxErrorException("NegativeIntegerArgument: limit should only have positive arguments: " + expressionText);
        }

        CypherAstBase expression = visitExpression(ctx.expression());
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
    public CypherBoolean visitBooleanLiteral(CypherParser.BooleanLiteralContext ctx) {
        if (ctx.TRUE() != null) {
            return new CypherBoolean(true);
        }
        if (ctx.FALSE() != null) {
            return new CypherBoolean(false);
        }
        throw new VertexiumException("unexpected boolean: " + ctx.getText());
    }

    @Override
    public CypherOrderBy visitOrder(CypherParser.OrderContext ctx) {
        List<CypherSortItem> sortItems = ctx.sortItem().stream()
                .map(this::visitSortItem)
                .collect(Collectors.toList());
        return new CypherOrderBy(sortItems);
    }

    @Override
    public CypherIdInColl visitIdInColl(CypherParser.IdInCollContext ctx) {
        CypherVariable variable = visitVariable(ctx.variable());
        CypherAstBase expression = visitExpression(ctx.expression());
        return new CypherIdInColl(variable, expression);
    }

    @Override
    public CypherRelTypeName visitRelTypeName(CypherParser.RelTypeNameContext ctx) {
        return new CypherRelTypeName(visitSymbolicName(ctx.symbolicName()).getValue());
    }

    @Override
    public CypherDouble visitDoubleLiteral(CypherParser.DoubleLiteralContext ctx) {
        return new CypherDouble(Double.parseDouble(ctx.getText()));
    }

    @Override
    public CypherAstBase visitDash(CypherParser.DashContext ctx) {
        throw new VertexiumCypherNotImplemented("Dash");
    }

    @Override
    public CypherAstBase visitNodeLabel(CypherParser.NodeLabelContext ctx) {
        throw new VertexiumCypherNotImplemented("NodeLabel");
    }

    @Override
    public CypherAstBase visitRightArrowHead(CypherParser.RightArrowHeadContext ctx) {
        throw new VertexiumCypherNotImplemented("RightArrowHead");
    }

    @Override
    public CypherAstBase visitPropertyExpression(CypherParser.PropertyExpressionContext ctx) {
        if (ctx.propertyLookup() != null) {
            return createLookup(ctx.atom(), ctx.propertyLookup(), null);
        }
        return visitAtom(ctx.atom());
    }

    @Override
    public CypherRemoveItem visitRemoveItem(CypherParser.RemoveItemContext ctx) {
        if (ctx.propertyExpression() != null) {
            return new CypherRemovePropertyExpressionItem(visitPropertyExpression(ctx.propertyExpression()));
        } else {
            return new CypherRemoveLabelItem(
                    visitVariable(ctx.variable()),
                    visitNodeLabels(ctx.nodeLabels())
            );
        }
    }

    @Override
    public CypherListLiteral<CypherAstBase> visitListLiteral(CypherParser.ListLiteralContext ctx) {
        return visitExpressions(ctx.expression());
    }

    @Override
    public CypherSkip visitSkip(CypherParser.SkipContext ctx) {
        CypherAstBase expression = visitExpression(ctx.expression());
        return new CypherSkip(expression);
    }

    @Override
    public CypherAstBase visitLeftArrowHead(CypherParser.LeftArrowHeadContext ctx) {
        throw new VertexiumCypherNotImplemented("LeftArrowHead");
    }

    @Override
    public CypherAstBase visitDelete(CypherParser.DeleteContext ctx) {
        boolean detach = ctx.DETACH() != null;
        CypherListLiteral<CypherAstBase> expressions = visitExpressions(ctx.expression());
        return new CypherDeleteClause(expressions, detach);
    }

    @Override
    public CypherAstBase visitRemove(CypherParser.RemoveContext ctx) {
        List<CypherRemoveItem> removeItems = ctx.removeItem().stream()
                .map(this::visitRemoveItem)
                .collect(Collectors.toList());
        return new CypherRemoveClause(removeItems);
    }

    @Override
    public CypherAstBase visitFunctionInvocation(CypherParser.FunctionInvocationContext ctx) {
        String functionName = visitFunctionName(ctx.functionName()).getValue();
        CypherFunction fn = compilerContext.getFunction(functionName);
        if (fn == null) {
            throw new VertexiumCypherSyntaxErrorException("UnknownFunction: Could not find function with name \"" + functionName + "\"");
        }
        boolean distinct = ctx.DISTINCT() != null;
        CypherListLiteral<CypherAstBase> argumentsList = visitExpressions(ctx.expression());
        CypherAstBase[] arguments = argumentsList.toArray(new CypherAstBase[argumentsList.size()]);
        fn.compile(compilerContext, arguments);
        return new CypherFunctionInvocation(functionName, distinct, arguments);
    }

    @Override
    public CypherAstBase visitListComprehension(CypherParser.ListComprehensionContext ctx) {
        CypherFilterExpression filterExpression = visitFilterExpression(ctx.filterExpression());
        CypherAstBase expression = ctx.expression() == null ? null : visitExpression(ctx.expression());
        return new CypherListComprehension(filterExpression, expression);
    }

    @Override
    public CypherStatement visitCypher(CypherParser.CypherContext ctx) {
        return visitStatement(ctx.statement());
    }

    @Override
    public CypherAstBase visitParameter(CypherParser.ParameterContext ctx) {
        if (ctx.symbolicName() != null) {
            String parameterName = visitSymbolicName(ctx.symbolicName()).getValue();
            return new CypherNameParameter(parameterName);
        }
        if (ctx.DecimalInteger() != null) {
            return new CypherIndexedParameter(Integer.parseInt(ctx.DecimalInteger().getText()));
        }
        throw new VertexiumCypherNotImplemented("Parameter");
    }

    @Override
    public CypherMergeAction visitMergeAction(CypherParser.MergeActionContext ctx) {
        CypherSetClause set = visitSet(ctx.set());
        if (ctx.CREATE() != null) {
            return new CypherMergeActionCreate(set);
        } else if (ctx.MATCH() != null) {
            return new CypherMergeActionMatch(set);
        } else {
            throw new VertexiumCypherSyntaxErrorException("Expected ON CREATE or ON MATCH");
        }
    }

    @Override
    public CypherSortItem visitSortItem(CypherParser.SortItemContext ctx) {
        CypherAstBase expr = visitExpression(ctx.expression());
        CypherSortItem.Direction direction;
        if (ctx.DESC() != null || ctx.DESCENDING() != null) {
            direction = CypherSortItem.Direction.DESCENDING;
        } else {
            direction = CypherSortItem.Direction.ASCENDING;
        }
        return new CypherSortItem(expr, direction);
    }

    @Override
    public CypherSetItem visitSetItem(CypherParser.SetItemContext ctx) {
        if (ctx.propertyExpression() != null) {
            CypherAstBase lookup = visitPropertyExpression(ctx.propertyExpression());
            if (!(lookup instanceof CypherLookup)) {
                throw new VertexiumException("expected " + CypherLookup.class.getName() + " found " + lookup.getClass().getName());
            }
            return new CypherSetProperty(
                    (CypherLookup) lookup,
                    visitExpression(ctx.expression())
            );
        }

        if (ctx.nodeLabels() != null) {
            return new CypherSetNodeLabels(
                    visitVariable(ctx.variable()),
                    visitNodeLabels(ctx.nodeLabels())
            );
        }

        CypherSetItem.Op op = getSetItemOp(ctx);
        return new CypherSetVariable(
                visitVariable(ctx.variable()),
                op,
                visitExpression(ctx.expression())
        );
    }

    private CypherSetItem.Op getSetItemOp(CypherParser.SetItemContext ctx) {
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
    public CypherSetClause visitSet(CypherParser.SetContext ctx) {
        return new CypherSetClause(ctx.setItem().stream().map(this::visitSetItem).collect(Collectors.toList()));
    }

    @Override
    public CypherString visitFunctionName(CypherParser.FunctionNameContext ctx) {
        if (ctx.UnescapedSymbolicName() != null) {
            return visitUnescapedSymbolicName(ctx.UnescapedSymbolicName());
        } else if (ctx.EscapedSymbolicName() != null) {
            return visitEscapedSymbolicName(ctx.EscapedSymbolicName());
        } else if (ctx.COUNT() != null) {
            return new CypherString("count");
        } else {
            throw new VertexiumException("unexpected function name: " + ctx.getText());
        }
    }

    private CypherString visitEscapedSymbolicName(TerminalNode escapedSymbolicName) {
        String text = escapedSymbolicName.getText();
        text = text.substring(1, text.length() - 1);
        return new CypherString(text);
    }

    private CypherString visitUnescapedSymbolicName(TerminalNode unescapedSymbolicName) {
        return new CypherString(unescapedSymbolicName.getText());
    }

    @Override
    public CypherRelationshipsPattern visitRelationshipsPattern(CypherParser.RelationshipsPatternContext ctx) {
        CypherNodePattern nodePattern = visitNodePattern(ctx.nodePattern());
        List<CypherElementPattern> patternElementChains = visitPatternElementChainList(nodePattern, ctx.patternElementChain());
        return new CypherRelationshipsPattern(nodePattern, patternElementChains);
    }

    private CypherAstBase visitUnions(CypherQuery left, List<CypherParser.UnionContext> unions) {
        if (unions.size() == 0) {
            return left;
        }
        CypherParser.UnionContext firstUnion = unions.get(0);
        boolean all = firstUnion.ALL() != null;
        CypherQuery right = visitSingleQuery(firstUnion.singleQuery());
        return new CypherUnion(left, visitUnions(right, unions.subList(1, unions.size())), all);
    }

    @Override
    public CypherUnion visitUnion(CypherParser.UnionContext ctx) {
        throw new VertexiumCypherNotImplemented("Union");
    }

    @Override
    public CypherAstBase visitRelationshipDetail(CypherParser.RelationshipDetailContext ctx) {
        throw new VertexiumCypherNotImplemented("RelationshipDetail");
    }

    @Override
    public CypherRangeLiteral visitRangeLiteral(CypherParser.RangeLiteralContext ctx) {
        Integer from = null;
        Integer to = null;
        boolean seenDotDot = false;
        for (ParseTree child : ctx.children) {
            if (child instanceof CypherParser.IntegerLiteralContext) {
                int i = visitIntegerLiteral((CypherParser.IntegerLiteralContext) child).getIntValue();
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
    public CypherFilterExpression visitFilterExpression(CypherParser.FilterExpressionContext ctx) {
        CypherIdInColl idInCol = visitIdInColl(ctx.idInColl());
        CypherAstBase where = ctx.where() == null ? null : visitWhere(ctx.where());
        return new CypherFilterExpression(idInCol, where);
    }

    @Override
    public CypherInteger visitIntegerLiteral(CypherParser.IntegerLiteralContext ctx) {
        try {
            return new CypherInteger(Long.decode(ctx.getText()));
        } catch (NumberFormatException ex) {
            throw new VertexiumException("could not parse \"" + ctx.getText() + "\" into integer");
        }
    }

    @Override
    public CypherListLiteral<CypherRelTypeName> visitRelationshipTypes(CypherParser.RelationshipTypesContext ctx) {
        return ctx.relTypeName().stream()
                .map(this::visitRelTypeName)
                .collect(CypherListLiteral.collect());
    }

    @Override
    public CypherLiteral visitNumberLiteral(CypherParser.NumberLiteralContext ctx) {
        return (CypherLiteral) super.visitNumberLiteral(ctx);
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

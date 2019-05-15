package org.vertexium.cypher;

import org.vertexium.*;
import org.vertexium.cypher.ast.model.CypherNodePattern;
import org.vertexium.cypher.ast.model.CypherRelationshipPattern;
import org.vertexium.cypher.executor.*;
import org.vertexium.cypher.executor.models.match.MatchConstraint;
import org.vertexium.cypher.functions.CypherFunction;
import org.vertexium.cypher.functions.aggregate.*;
import org.vertexium.cypher.functions.date.DayFunction;
import org.vertexium.cypher.functions.date.MonthFunction;
import org.vertexium.cypher.functions.date.YearFunction;
import org.vertexium.cypher.functions.list.*;
import org.vertexium.cypher.functions.math.*;
import org.vertexium.cypher.functions.predicate.*;
import org.vertexium.cypher.functions.scalar.*;
import org.vertexium.cypher.functions.spatial.DistanceFunction;
import org.vertexium.cypher.functions.spatial.PointFunction;
import org.vertexium.cypher.functions.string.*;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.vertexium.util.StreamUtils.stream;

public abstract class VertexiumCypherQueryContext {
    private final Graph graph;
    private final Map<String, Object> parameters = new HashMap<>();
    private final Map<String, CypherFunction> functions = new HashMap<>();
    private final User user;
    private final ExpressionExecutor expressionExecutor;
    private final CreateClauseExecutor createClauseExecutor;
    private final ReturnClauseExecutor returnClauseExecutor;
    private final MatchClauseExecutor matchClauseExecutor;
    private final UnwindClauseExecutor unwindClauseExecutor;
    private final WithClauseExecutor withClauseExecutor;
    private final MergeClauseExecutor mergeClauseExecutor;
    private final DeleteClauseExecutor deleteClauseExecutor;
    private final SetClauseExecutor setClauseExecutor;
    private final RemoveClauseExecutor removeClauseExecutor;
    private final CypherResultWriter resultWriter;
    private final Map<MatchConstraint<?, ?>, Long> totalHitsByMatchConstraint = new HashMap<>();

    public VertexiumCypherQueryContext(Graph graph, User user) {
        this.graph = graph;
        this.user = user;

        // aggregate
        addFunction("avg", new AverageFunction());
        addFunction("collect", new CollectFunction());
        addFunction("count", new CountFunction());
        addFunction("max", new MaxFunction());
        addFunction("min", new MinFunction());
        addFunction("percentileCont", new PercentileContFunction());
        addFunction("percentileDisc", new PercentileDiscFunction());
        addFunction("stDev", new StandardDeviationFunction());
        addFunction("stDevP", new StandardDeviationPopulationFunction());
        addFunction("sum", new SumFunction());

        // list
        addFunction("extract", new ExtractFunction());
        addFunction("filter", new FilterFunction());
        addFunction("keys", new KeysFunction());
        addFunction("labels", new LabelsFunction());
        addFunction("nodes", new NodesFunction());
        addFunction("range", new RangeFunction());
        addFunction("reduce", new ReduceFunction());
        addFunction("relationships", new RelationshipsFunction());
        addFunction("tail", new TailFunction());

        // math
        addFunction("abs", new AbsFunction());
        addFunction("ceil", new CeilFunction());
        addFunction("floor", new FloorFunction());
        addFunction("rand", new RandFunction());
        addFunction("round", new RoundFunction());
        addFunction("sign", new SignFunction());
        addFunction("e", new EFunction());
        addFunction("exp", new ExpFunction());
        addFunction("log", new LogFunction());
        addFunction("log10", new Log10Function());
        addFunction("sqrt", new SquareRootFunction());
        addFunction("acos", new ACosFunction());
        addFunction("asin", new ASinFunction());
        addFunction("atan", new ATanFunction());
        addFunction("atan2", new ATan2Function());
        addFunction("cos", new CosFunction());
        addFunction("cot", new CotFunction());
        addFunction("degrees", new DegreesFunction());
        addFunction("haversin", new HaversinFunction());
        addFunction("pi", new PiFunction());
        addFunction("radians", new RadiansFunction());
        addFunction("sin", new SinFunction());
        addFunction("tan", new TanFunction());

        // predicate
        addFunction("all", new AllFunction());
        addFunction("any", new AnyFunction());
        addFunction("exists", new ExistsFunction());
        addFunction("none", new NoneFunction());
        addFunction("single", new SingleFunction());

        // scalar
        addFunction("coalesce", new CoalesceFunction());
        addFunction("endNode", new EndNodeFunction());
        addFunction("head", new HeadFunction());
        addFunction("id", new IdFunction());
        addFunction("last", new LastFunction());
        addFunction("length", new LengthFunction());
        addFunction("properties", new PropertiesFunction());
        addFunction("size", new SizeFunction());
        addFunction("startNode", new StartNodeFunction());
        addFunction("timestamp", new TimestampFunction());
        addFunction("toBoolean", new ToBooleanFunction());
        addFunction("toFloat", new ToFloatFunction());
        addFunction("toInteger", new ToIntegerFunction());
        addFunction("type", new TypeFunction());

        // spatial
        addFunction("distance", new DistanceFunction());
        addFunction("point", new PointFunction());

        // string
        addFunction("left", new LeftFunction());
        addFunction("lTrim", new LTrimFunction());
        addFunction("replace", new ReplaceFunction());
        addFunction("reverse", new ReverseFunction());
        addFunction("right", new RightFunction());
        addFunction("rTrim", new RTrimFunction());
        addFunction("split", new SplitFunction());
        addFunction("substring", new SubstringFunction());
        addFunction("toLower", new ToLowerFunction());
        addFunction("lower", new ToLowerFunction());
        addFunction("toString", new ToStringFunction());
        addFunction("toUpper", new ToUpperFunction());
        addFunction("upper", new ToUpperFunction());
        addFunction("trim", new TrimFunction());

        // date
        addFunction("year", new YearFunction());
        addFunction("month", new MonthFunction());
        addFunction("day", new DayFunction());

        this.resultWriter = new CypherResultWriter();
        this.expressionExecutor = new ExpressionExecutor();
        this.createClauseExecutor = new CreateClauseExecutor(expressionExecutor);
        this.returnClauseExecutor = new ReturnClauseExecutor(expressionExecutor);
        this.matchClauseExecutor = new MatchClauseExecutor();
        this.unwindClauseExecutor = new UnwindClauseExecutor(expressionExecutor);
        this.withClauseExecutor = new WithClauseExecutor();
        this.mergeClauseExecutor = new MergeClauseExecutor();
        this.deleteClauseExecutor = new DeleteClauseExecutor(expressionExecutor);
        this.setClauseExecutor = new SetClauseExecutor();
        this.removeClauseExecutor = new RemoveClauseExecutor();
    }

    public Graph getGraph() {
        return graph;
    }

    public abstract Visibility calculateVertexVisibility(CypherNodePattern nodePattern, ExpressionScope scope);

    public User getUser() {
        return user;
    }

    public abstract String getLabelPropertyName();

    public abstract <T extends Element> void setLabelProperty(ElementMutation<T> m, String label);

    public abstract void removeLabel(ExistingElementMutation<Vertex> vertex, String label);

    public abstract <T extends Element> void setProperty(ElementMutation<T> m, String propertyName, Object value);

    public abstract void removeProperty(ElementMutation<Element> m, String propertyName);

    public abstract String calculateEdgeLabel(
        CypherRelationshipPattern relationshipPattern,
        Vertex outVertex,
        Vertex inVertex,
        ExpressionScope scope
    );

    public abstract Visibility calculateEdgeVisibility(
        CypherRelationshipPattern relationshipPattern,
        Vertex outVertex,
        Vertex inVertex,
        ExpressionScope scope
    );

    public abstract boolean isLabelProperty(Property property);

    public abstract Set<String> getVertexLabels(Vertex vertex);

    public void setParameter(String name, Object value) {
        parameters.put(name, value);
    }

    public CypherFunction getFunction(String functionName) {
        return functions.get(functionName.toLowerCase());
    }

    public void deleteEdge(Edge edge) {
        edge.prepareMutation()
            .deleteElement()
            .save(getUser());
    }

    public void deleteVertex(Vertex vertex) {
        vertex.prepareMutation()
            .deleteElement()
            .save(getUser());
    }

    public String saveEdge(ElementMutation<Edge> m) {
        return m.save(getUser());
    }

    public String saveVertex(ElementMutation<Vertex> m) {
        return m.save(getUser());
    }

    @SuppressWarnings("unchecked")
    public <T extends Element> void saveElement(ExistingElementMutation<T> m) {
        if (m.getElement() instanceof Edge) {
            saveEdge((ElementMutation<Edge>) m);
        } else if (m.getElement() instanceof Vertex) {
            saveVertex((ElementMutation<Vertex>) m);
        } else {
            throw new VertexiumException("unexpected element type: " + m.getElement().getClass().getName());
        }
    }

    public ExpressionExecutor getExpressionExecutor() {
        return expressionExecutor;
    }

    public CreateClauseExecutor getCreateClauseExecutor() {
        return createClauseExecutor;
    }

    public ReturnClauseExecutor getReturnClauseExecutor() {
        return returnClauseExecutor;
    }

    public MatchClauseExecutor getMatchClauseExecutor() {
        return matchClauseExecutor;
    }

    public UnwindClauseExecutor getUnwindClauseExecutor() {
        return unwindClauseExecutor;
    }

    public WithClauseExecutor getWithClauseExecutor() {
        return withClauseExecutor;
    }

    public MergeClauseExecutor getMergeClauseExecutor() {
        return mergeClauseExecutor;
    }

    public DeleteClauseExecutor getDeleteClauseExecutor() {
        return deleteClauseExecutor;
    }

    public SetClauseExecutor getSetClauseExecutor() {
        return setClauseExecutor;
    }

    public RemoveClauseExecutor getRemoveClauseExecutor() {
        return removeClauseExecutor;
    }

    public CypherResultWriter getResultWriter() {
        return resultWriter;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public Map<String, CypherFunction> getFunctions() {
        return functions;
    }

    public void addFunction(String name, CypherFunction fn) {
        this.functions.put(name.toLowerCase(), fn);
    }

    public Set<String> getKeys(Element element) {
        Set<String> results = new HashSet<>();
        for (Property property : element.getProperties()) {
            if (!isLabelProperty(property)) {
                results.add(property.getName());
            }
        }
        return results;
    }

    public Map<String, Object> getElementPropertiesAsMap(Element element) {
        return stream(element.getProperties())
            .filter(p -> !isLabelProperty(p))
            .collect(Collectors.toMap(Property::getName, Property::getValue));
    }

    public FetchHints getFetchHints() {
        return FetchHints.ALL;
    }

    public abstract int getMaxUnboundedRange();

    public String calculateVertexId(CypherNodePattern nodePattern, ExpressionScope scope) {
        return null;
    }

    public String calculateEdgeId(CypherRelationshipPattern relationshipPattern, ExpressionScope scope) {
        return null;
    }

    public String normalizeLabelName(String labelName) {
        return labelName;
    }

    public String normalizePropertyName(String propertyName) {
        return propertyName;
    }

    public Long getTotalHitsForMatchConstraint(
        MatchConstraint<?, ?> matchConstraint,
        Function<MatchConstraint<?, ?>, Long> computeFn
    ) {
        return totalHitsByMatchConstraint.computeIfAbsent(matchConstraint, computeFn);
    }
}

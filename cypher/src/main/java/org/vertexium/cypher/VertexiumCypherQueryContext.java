package org.vertexium.cypher;

import org.vertexium.*;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.executionPlan.*;
import org.vertexium.cypher.functions.CypherFunction;
import org.vertexium.cypher.functions.aggregate.*;
import org.vertexium.cypher.functions.date.*;
import org.vertexium.cypher.functions.date.duration.DurationBetweenFunction;
import org.vertexium.cypher.functions.date.duration.DurationInDaysFunction;
import org.vertexium.cypher.functions.date.duration.DurationInMonthsFunction;
import org.vertexium.cypher.functions.date.duration.DurationInSecondsFunction;
import org.vertexium.cypher.functions.list.*;
import org.vertexium.cypher.functions.math.*;
import org.vertexium.cypher.functions.predicate.*;
import org.vertexium.cypher.functions.scalar.*;
import org.vertexium.cypher.functions.spatial.DistanceFunction;
import org.vertexium.cypher.functions.spatial.PointFunction;
import org.vertexium.cypher.functions.string.*;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.vertexium.util.StreamUtils.stream;

public abstract class VertexiumCypherQueryContext {
    private final Graph graph;
    private final Map<String, Object> parameters = new HashMap<>();
    private final Map<String, CypherFunction> functions = new HashMap<>();
    private final Authorizations authorizations;
    private final CypherResultWriter resultWriter;
    private ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder();
    private ExecutionPlan currentlyExecutingPlan;

    public VertexiumCypherQueryContext(Graph graph, Authorizations authorizations) {
        this.graph = graph;
        this.authorizations = authorizations;

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

        addFunction("negate", new NegateFunction());

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

        addFunction("isNull", new IsNullFunction());
        addFunction("isNotNull", new IsNotNullFunction());

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

        addFunction("startsWith", new StartsWithFunction());
        addFunction("endsWith", new EndsWithFunction());
        addFunction("contains", new ContainsFunction());

        // date
        addFunction("year", new YearFunction());
        addFunction("month", new MonthFunction());
        addFunction("day", new DayFunction());
        addFunction("localdatetime", new LocalDateTimeFunction());
        addFunction("datetime", new DateTimeFunction());
        addFunction("date", new DateFunction());
        addFunction("localtime", new LocalTimeFunction());
        addFunction("time", new TimeFunction());
        addFunction("duration.between", new DurationBetweenFunction());
        addFunction("duration.inMonths", new DurationInMonthsFunction());
        addFunction("duration.inDays", new DurationInDaysFunction());
        addFunction("duration.inSeconds", new DurationInSecondsFunction());

        this.resultWriter = new CypherResultWriter();
    }

    public Graph getGraph() {
        return graph;
    }

    public abstract Visibility calculateVertexVisibility(CreateNodePatternExecutionStep nodePattern, CypherResultRow row);

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public abstract String getLabelPropertyName();

    public abstract <T extends Element> void setLabelProperty(ElementMutation<T> m, String label);

    public abstract void removeLabel(ExistingElementMutation<Vertex> vertex, String label);

    public abstract <T extends Element> void setProperty(ElementMutation<T> m, String propertyName, Object value);

    public abstract void setProperty(Element element, String propertyName, Object value);

    public abstract void removeProperty(Element element, Property prop);

    public abstract void removeProperty(Element element, String propName);

    public abstract String calculateEdgeLabel(
        CreateRelationshipPatternExecutionStep relationshipPattern,
        Vertex outVertex,
        Vertex inVertex,
        CypherResultRow row);

    public abstract Visibility calculateEdgeVisibility(
        CreateRelationshipPatternExecutionStep relationshipPattern,
        Vertex outVertex,
        Vertex inVertex,
        CypherResultRow row);

    public abstract boolean isLabelProperty(Property property);

    public abstract Set<String> getVertexLabels(Vertex vertex);

    public void setParameter(String name, Object value) {
        parameters.put(name, value);
    }

    public void deleteEdge(Edge edge) {
        getGraph().deleteEdge(edge, getAuthorizations());
    }

    public void deleteVertex(Vertex vertex) {
        getGraph().deleteVertex(vertex, getAuthorizations());
    }

    public Edge saveEdge(ElementMutation<Edge> m) {
        return m.save(getAuthorizations());
    }

    public Vertex saveVertex(ElementMutation<Vertex> m) {
        return m.save(getAuthorizations());
    }

    @SuppressWarnings("unchecked")
    public <T extends Element> T saveElement(ExistingElementMutation<T> m) {
        if (m.getElement() instanceof Vertex) {
            return (T) saveVertex((ElementMutation<Vertex>) m);
        } else if (m.getElement() instanceof Edge) {
            return (T) saveEdge((ElementMutation<Edge>) m);
        } else {
            throw new VertexiumCypherNotImplemented("Cannot save: " + m.getElement().getClass().getName());
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Element> T saveElement(ElementType type, ElementMutation<T> m) {
        switch (type) {
            case VERTEX:
                return (T) saveVertex((ElementMutation<Vertex>) m);
            case EDGE:
                return (T) saveEdge((ElementMutation<Edge>) m);
            default:
                throw new VertexiumCypherNotImplemented("Cannot save: " + type);
        }
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

    public String calculateVertexId(CreateNodePatternExecutionStep nodePattern, CypherResultRow row) {
        return null;
    }

    public String calculateEdgeId(CreateRelationshipPatternExecutionStep relationshipPattern, CypherResultRow row) {
        return null;
    }

    public String normalizeLabelName(String labelName) {
        return labelName;
    }

    public String normalizePropertyName(String propertyName) {
        return propertyName;
    }

    public ExecutionStepWithResultName createFunctionExecutionStep(
        String functionName,
        String resultName,
        boolean distinct,
        ExecutionStepWithResultName[] argumentsExecutionStep
    ) {
        CypherFunction fn = functions.get(functionName.toLowerCase());
        if (fn == null) {
            throw new VertexiumCypherException(String.format("Function \"%s\" not found", functionName));
        }
        return fn.create(resultName, distinct, argumentsExecutionStep);
    }

    public ExecutionPlanBuilder getExecutionPlanBuilder() {
        return executionPlanBuilder;
    }

    public Object getParameter(String parameterName) {
        return getParameters().get(parameterName);
    }

    public abstract void defineProperty(String propertyName, Object value);

    public void setCurrentlyExecutingPlan(ExecutionPlan currentlyExecutingPlan) {
        this.currentlyExecutingPlan = currentlyExecutingPlan;
    }

    public ExecutionPlan getCurrentlyExecutingPlan() {
        return currentlyExecutingPlan;
    }

    public ZoneId getZoneId() {
        return ZoneId.of("UTC");
    }

    public ZonedDateTime getNow() {
        return ZonedDateTime.now(getZoneId());
    }
}

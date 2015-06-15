package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.util.FilterIterable;

import java.util.EnumSet;
import java.util.Map;

public abstract class QueryBase implements Query, SimilarToGraphQuery {
    private final Graph graph;
    private final Map<String, PropertyDefinition> propertyDefinitions;
    private final QueryParameters parameters;

    protected QueryBase(Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions,  Authorizations authorizations) {
        this.graph = graph;
        this.propertyDefinitions = propertyDefinitions;
        this.parameters = new QueryStringQueryParameters(queryString, authorizations);
    }

    protected QueryBase(Graph graph, String[] similarToFields, String similarToText, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        this.graph = graph;
        this.propertyDefinitions = propertyDefinitions;
        this.parameters = new SimilarToTextQueryParameters(similarToFields, similarToText, authorizations);
    }

    @Override
    public Iterable<Vertex> vertices() {
        return vertices(FetchHint.ALL);
    }

    @Override
    public abstract Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    @Override
    public Iterable<Edge> edges() {
        return edges(FetchHint.ALL);
    }

    @Override
    public abstract Iterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    @Override
    public Iterable<Edge> edges(final String label, EnumSet<FetchHint> fetchHints) {
        return new FilterIterable<Edge>(edges()) {
            @Override
            protected boolean isIncluded(Edge o) {
                return label.equals(o.getLabel());
            }
        };
    }

    @Override
    public Iterable<Edge> edges(final String label) {
        return edges(label, FetchHint.ALL);
    }

    @Override
    public <T> Query range(String propertyName, T startValue, T endValue) {
        return range(propertyName, startValue, true, endValue, true);
    }

    @Override
    public <T> Query range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, inclusiveStartValue ? Compare.GREATER_THAN_EQUAL : Compare.GREATER_THAN, startValue, this.propertyDefinitions));
        this.parameters.addHasContainer(new HasValueContainer(propertyName, inclusiveEndValue ? Compare.LESS_THAN_EQUAL : Compare.LESS_THAN, endValue, this.propertyDefinitions));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, Compare.EQUAL, value, this.propertyDefinitions));
        return this;
    }

    @Override
    public <T> Query hasNot(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, Contains.NOT_IN, new Object[]{value}, this.propertyDefinitions));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, predicate, value, this.propertyDefinitions));
        return this;
    }

    @Override
    public Query has(String propertyName) {
        this.parameters.addHasContainer(new HasPropertyContainer(propertyName));
        return this;
    }

    @Override
    public Query hasNot(String propertyName) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(propertyName));
        return this;
    }

    @Override
    public Query skip(int count) {
        this.parameters.setSkip(count);
        return this;
    }

    @Override
    public Query limit(int count) {
        this.parameters.setLimit(count);
        return this;
    }

    public Graph getGraph() {
        return graph;
    }

    public QueryParameters getParameters() {
        return parameters;
    }

    protected Map<String, PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitions;
    }

    public static abstract class HasContainer {
        public abstract boolean isMatch(Element elem);
    }

    public static class HasValueContainer extends HasContainer {
        public String key;
        public Object value;
        public Predicate predicate;
        private final Map<String, PropertyDefinition> propertyDefinitions;

        public HasValueContainer(final String key, final Predicate predicate, final Object value, Map<String, PropertyDefinition> propertyDefinitions) {
            this.key = key;
            this.value = value;
            this.predicate = predicate;
            this.propertyDefinitions = propertyDefinitions;
        }

        public boolean isMatch(Element elem) {
            return this.predicate.evaluate(elem.getProperties(this.key), this.value, this.propertyDefinitions);
        }
    }

    public static class HasPropertyContainer extends HasContainer {
        private final String key;

        public HasPropertyContainer(String key) {
            this.key = key;
        }

        @Override
        public boolean isMatch(Element elem) {
            for (Property prop : elem.getProperties()) {
                if (prop.getName().equals(this.key)) {
                    return true;
                }
            }
            return false;
        }

        public String getKey() {
            return key;
        }
    }

    public static class HasNotPropertyContainer extends HasContainer {
        private final String key;

        public HasNotPropertyContainer(String key) {
            this.key = key;
        }

        @Override
        public boolean isMatch(Element elem) {
            for (Property prop : elem.getProperties()) {
                if (prop.getName().equals(this.key)) {
                    return false;
                }
            }
            return true;
        }

        public String getKey() {
            return key;
        }
    }

    @Override
    public SimilarToGraphQuery minTermFrequency(int minTermFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinTermFrequency(minTermFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery maxQueryTerms(int maxQueryTerms) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxQueryTerms(maxQueryTerms);
        return this;
    }

    @Override
    public SimilarToGraphQuery minDocFrequency(int minDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinDocFrequency(minDocFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery maxDocFrequency(int maxDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxDocFrequency(maxDocFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery percentTermsToMatch(float percentTermsToMatch) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setPercentTermsToMatch(percentTermsToMatch);
        return this;
    }

    @Override
    public SimilarToGraphQuery boost(float boost) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setBoost(boost);
        return this;
    }
}

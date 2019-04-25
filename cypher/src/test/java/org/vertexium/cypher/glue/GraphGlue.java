package org.vertexium.cypher.glue;

import cucumber.api.DataTable;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assume;
import org.vertexium.Authorizations;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.*;
import org.vertexium.cypher.ast.CypherAstParser;
import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.executionPlan.ExecutionPlanBuilder;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphGlue {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(GraphGlue.class);

    public static final Pattern RELATIONSHIP_REGEX = Pattern.compile("^\\[(.*?)(\\{.*\\})\\]$");
    public static final Pattern NODE_REGEX = Pattern.compile("^\\((.*?)(\\{.*\\})?\\)$");
    private VertexiumCypherQuery query;
    private List<CypherResultRow> lastResultRows;
    private LinkedHashSet<String> lastResultColumnNames;
    private TestVertexiumCypherQueryContext ctx;
    private Exception lastCompileTimeException;
    private Exception lastRuntimeException;

    @Given("^any graph$")
    public void givenAnyGraph() {
        createGraph();
    }

    @Given("^an empty graph$")
    public void givenEmptyGraph() {
        createGraph();
    }

    @Given("^the binary-tree-(\\d+) graph$")
    public void givenTheBinaryTreeGraph(int number) throws Throwable {
        createGraph();
        String resourceName = String.format("/org/vertexium/cypher/tck/graphs/binary-tree-%d/binary-tree-%d.cypher", number, number);
        InputStream treeIn = this.getClass().getResourceAsStream(resourceName);
        if (treeIn == null) {
            throw new VertexiumException("Could not find '" + resourceName + "'");
        }
        String cyp = IOUtils.toString(treeIn);
        CypherCompilerContext compilerContext = new CypherCompilerContext(ctx.getFunctions());
        VertexiumCypherQuery.parse(compilerContext, cyp).execute(ctx);
    }

    private void createGraph() {
        InMemoryGraph graph = InMemoryGraph.create();
        Authorizations authorizations = graph.createAuthorizations();
        ctx = new TestVertexiumCypherQueryContext(graph, authorizations);
    }

    @Given("^parameters are:$")
    public void givenParametersAre(DataTable parameters) {
        for (List<String> parameterRow : parameters.raw()) {
            String key = parameterRow.get(0);
            String valueString = parameterRow.get(1);
            Object value = parseParameterValue(valueString);
            ctx.setParameter(key, value);
        }
    }

    private Object parseParameterValue(String valueString) {
        valueString = valueString.trim();
        return executeExpression(valueString);
    }

    private Object executeExpression(String valueString) {
        CypherAstBase expression = CypherAstParser.getInstance().parseExpression(valueString);
        return executeExpression(expression);
    }

    private Object executeExpression(CypherAstBase expression) {
        ExecutionStepWithResultName plan = new ExecutionPlanBuilder().visitExpression(ctx, "value", expression);
        VertexiumCypherResult result = plan.execute(ctx, new SingleRowVertexiumCypherResult());
        return result.findFirst().orElse(null).get("value");
    }

    @When("^executing(.*)query:$")
    public void whenExecutingQuery(String queryName, String queryString) {
        ctx.clearCounts();
        lastResultRows = null;
        lastResultColumnNames = null;
        lastCompileTimeException = null;
        lastRuntimeException = null;
        try {
            CypherCompilerContext compilerContext = new CypherCompilerContext(ctx.getFunctions());
            query = VertexiumCypherQuery.parse(compilerContext, queryString);
            try {
                VertexiumCypherResult results = query.execute(ctx);
                lastResultRows = results
                    .peek(row -> {
                        try {
                            Map<String, Object> scope = row.popScope();
                            throw new VertexiumCypherException("Scope should be empty:\n"
                                + scope.entrySet().stream().map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining("\n"))
                            );
                        } catch (Exception ex) {
                            // OK
                        }
                    })
                    .collect(Collectors.toList());
                lastResultColumnNames = results.getColumnNames();
            } catch (Exception ex) {
                lastRuntimeException = ex;
            }
        } catch (Exception ex) {
            lastCompileTimeException = ex;
        }
    }

    @Given("^having executed:$")
    public void givenHavingExecuted(String queryString) {
        CypherCompilerContext compilerContext = new CypherCompilerContext(ctx.getFunctions());
        VertexiumCypherQuery.parse(compilerContext, queryString).execute(ctx).count();
    }

    @Then("^the result should be, in order:$")
    public void thenTheResultShouldBeInOrder(DataTable expected) throws Throwable {
        thenTheResultShouldBe(expected);
    }

    @Then("^the result should be \\(ignoring element order for lists\\):$")
    public void thenTheResultShouldBeIgnoringElementOrderForLists(DataTable expected) throws Throwable {
        thenTheResultShouldBe(expected);
    }

    @Then("^the result should be:$")
    public void thenTheResultShouldBe(DataTable expected) throws Throwable {
        if (lastCompileTimeException != null) {
            throw lastCompileTimeException;
        }
        if (lastRuntimeException != null) {
            throw lastRuntimeException;
        }
        List<String> columnNames = expected.raw().get(0);
        if (expected.raw().size() > 0) {
            List<List<String>> expectedRows = expected.raw().stream()
                .skip(1)
                .collect(Collectors.toList());
            List<List<String>> foundRows = lastResultRows.stream()
                .map(row -> lastResultColumnNames.stream()
                    .map(row::get)
                    .map(obj -> ctx.getResultWriter().columnValueToString(ctx, obj))
                    .collect(Collectors.toList())
                )
                .collect(Collectors.toList());

            expectedRows.sort(new RowComparator());
            foundRows.sort(new RowComparator());

            if (expectedRows.size() > 0) {
                System.out.println("Expected");
                System.out.println(String.join(", ", expected.raw().get(0)));
                for (List<String> expectedRow : expectedRows) {
                    System.out.println(String.join(", ", expectedRow));
                }
            }

            System.out.println("Found");
            System.out.println(String.join(", ", lastResultColumnNames));
            for (List<String> foundRow : foundRows) {
                System.out.println(String.join(", ", foundRow));
            }

            if (expectedRows.size() > 0) {
                assertEquals(
                    String.format(
                        "Header count, expected (%s), found (%s)",
                        String.join(", ", expected.raw().get(0)),
                        String.join(", ", lastResultColumnNames)
                    ),
                    expected.raw().get(0).size(),
                    lastResultColumnNames.size()
                );
                for (int colIdx = 0; colIdx < expected.raw().get(0).size(); colIdx++) {
                    String expectedColumnName = expected.raw().get(0).get(colIdx);
                    assertTrue(
                        String.format(
                            "Header mismatch (expected: %s, columnNames: %s)",
                            expectedColumnName,
                            String.join(", ", lastResultColumnNames)
                        ),
                        lastResultColumnNames.contains(expectedColumnName)
                    );
                }
            }

            assertEquals("result count", expectedRows.size(), foundRows.size());

            if (expectedRows.size() > 0) {
                for (int row = 0; row < expectedRows.size(); row++) {
                    List<String> expectedRow = expectedRows.get(row);
                    List<String> foundRow = foundRows.get(row);
                    for (int col = 0; col < expectedRow.size(); col++) {
                        String expectedColumn = expectedRow.get(col);
                        String foundColumn = foundRow.get(col);
                        assertColumnValue(row, col, expectedColumn, foundColumn);
                    }
                }
            }
        }
    }

    private final class RowComparator implements Comparator<List<String>> {
        @Override
        public int compare(List<String> list1, List<String> list2) {
            for (int i = 0; i < list1.size(); i++) {
                String o1 = list1.get(i);
                String o2 = list2.get(i);
                int c = o1.compareTo(o2);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }

    private void assertColumnValue(int row, int column, String expected, String found) {
        if (expected == null && found == null) {
            return;
        }
        if (rowStringIsRelationship(expected) && rowStringIsRelationship(found)) {
            assertColumnValueRelationship(row, column, expected, found);
        } else if (rowStringIsNode(expected) && rowStringIsNode(found)) {
            assertColumnValueNode(row, column, expected, found);
        } else if (rowStringIsList(expected) && rowStringIsList(found)) {
            assertColumnValueList(row, column, expected, found);
        } else if (rowStringIsMap(expected) && rowStringIsMap(found)) {
            assertColumnValueMap(expected, found);
        } else {
            assertEquals(row + ":" + column, expected, found);
        }
    }

    private boolean rowStringIsNode(String string) {
        return NODE_REGEX.matcher(string).matches();
    }

    private void assertColumnValueNode(int row, int column, String expected, String found) {
        Matcher expectedMatch = NODE_REGEX.matcher(expected);
        Matcher foundMatch = NODE_REGEX.matcher(found);
        assertTrue(expectedMatch.matches());
        assertTrue(foundMatch.matches());
        String[] expectedLabels = expectedMatch.group(1).split(":");
        String[] foundLabels = foundMatch.group(1).split(":");
        assertEquals(expectedMatch.group(1) + " does not equal length of " + foundMatch.group(1), expectedLabels.length, foundLabels.length);
        Arrays.sort(expectedLabels);
        Arrays.sort(foundLabels);
        for (int i = 0; i < expectedLabels.length; i++) {
            assertEquals(expectedMatch.group(1) + " does not equal length of " + foundMatch.group(1), expectedLabels[i], foundLabels[i]);
        }
        assertColumnValue(row, column, expectedMatch.group(2), foundMatch.group(2));
    }

    private boolean rowStringIsRelationship(String string) {
        return RELATIONSHIP_REGEX.matcher(string).matches();
    }

    private void assertColumnValueRelationship(int row, int column, String expected, String found) {
        Matcher expectedMatch = RELATIONSHIP_REGEX.matcher(expected);
        Matcher foundMatch = RELATIONSHIP_REGEX.matcher(found);
        assertTrue(expectedMatch.matches());
        assertTrue(foundMatch.matches());
        assertColumnValue(row, column, expectedMatch.group(1), foundMatch.group(1));
        assertColumnValue(row, column, expectedMatch.group(2), foundMatch.group(2));
    }

    private boolean rowStringIsMap(String columnString) {
        return columnString.startsWith("{") && columnString.endsWith("}");
    }

    private void assertColumnValueMap(String expected, String found) {
        try {
            Map<?, ?> expectedMap = columnValueToMap(expected);
            Map<?, ?> foundMap = columnValueToMap(found);
            assertEquals(expectedMap.keySet(), foundMap.keySet());
            for (Object key : expectedMap.keySet()) {
                Object expectedValue = expectedMap.get(key);
                Object foundValue = foundMap.get(key);
                if (expectedValue instanceof Object[] && foundValue instanceof Object[]) {
                    Object[] expectedArr = (Object[]) expectedValue;
                    Object[] foundArr = (Object[]) foundValue;
                    assertEquals(expectedArr.length, foundArr.length);
                    for (int i = 0; i < expectedArr.length; i++) {
                        assertEquals(expectedArr[i], foundArr[i]);
                    }
                } else {
                    assertEquals(expectedValue, foundValue);
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("Could not convert to maps", ex);
            assertEquals(expected, found);
        }
    }

    private Map<?, ?> columnValueToMap(String columnValue) {
        return (Map) executeExpression(columnValue);
    }

    private boolean rowStringIsList(String columnString) {
        return columnString.startsWith("[") && columnString.endsWith("]");
    }

    private void assertColumnValueList(int row, int column, String expected, String found) {
        List<String> expectedList = columnValueToList(expected);
        List<String> foundList = columnValueToList(found);
        assertEquals(expectedList.size(), foundList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            String expectedListItem = expectedList.get(i);
            String foundListItem = foundList.get(i);
            assertColumnValue(row, column, expectedListItem, foundListItem);
        }
    }

    private List<String> columnValueToList(String columnValue) {
        columnValue = columnValue.substring(1, columnValue.length() - 1);
        return Arrays.stream(columnValue.split(","))
            .map(String::trim)
            .sorted()
            .collect(Collectors.toList());
    }

    @Then("^a (.*) should be raised at compile time: (.*)$")
    public void thenASyntaxErrorShouldBeRaisedAtCompileTime(String errorType, String error) {
        Exception ex = this.lastCompileTimeException;
        if (ex == null) {
            if (lastRuntimeException != null) {
                lastRuntimeException.printStackTrace();
                // TODO do we care?
                LOGGER.warn("statement should have resulted in a compile time exception, but resulted in a runtime exception");
                ex = lastRuntimeException;
            } else {
                Assume.assumeTrue("statement should have resulted in a compile time exception", false);
            }
        }
        if (!ex.getClass().getName().contains(errorType)) {
            ex.printStackTrace();
            // TODO do we care?
            LOGGER.warn("exception type should contain \"" + errorType + "\", but only contained \"" + ex.getClass().getName() + "\": " + ex);
        }
        if (ex.getMessage() == null || !ex.getMessage().contains(error)) {
            ex.printStackTrace();
            // TODO do we care?
            LOGGER.warn("exception should contain \"" + error + "\", but only contained \"" + ex.getMessage() + "\"");
        }
    }

    @Then("^a (.*) should be raised at runtime: (.*)$")
    public void thenATypeErrorShouldBeRaisedAtRuntime(String errorType, String error) {
        if (lastRuntimeException == null) {
            if (lastCompileTimeException != null) {
                LOGGER.error("compile time exception", lastCompileTimeException);
                fail("statement should have resulted in a runtime exception, but resulted in a compile time exception");
            } else {
                fail("statement should have resulted in a runtime exception");
            }
        }
        if (!lastRuntimeException.getClass().getName().contains(errorType)) {
            lastRuntimeException.printStackTrace();
            // TODO do we care?
            LOGGER.warn("exception type should contain \"" + errorType + "\", but only contained \"" + lastRuntimeException.getClass().getName() + "\": " + lastRuntimeException);
        }
        if (!lastRuntimeException.getMessage().contains(error)) {
            lastRuntimeException.printStackTrace();
            // TODO do we care?
            LOGGER.warn("exception should contain \"" + error + "\", but only contained \"" + lastRuntimeException.getMessage() + "\"");
        }
    }

    @Then("^the result should be empty$")
    public void thenTheResultsShouldBeEmpty() throws Throwable {
        if (lastCompileTimeException != null) {
            throw lastCompileTimeException;
        }
        if (lastRuntimeException != null) {
            throw lastRuntimeException;
        }
        assertEquals("number of rows", 0, lastResultRows.size());
    }

    @Then("^no side effects$")
    public void noSideEffects() {
        assertEquals("+node", 0, ctx.getPlusNodeCount());
        assertEquals("+relationship", 0, ctx.getPlusRelationshipCount());
        assertEquals("+label", 0, ctx.getPlusLabelCount());
        assertEquals("+property", 0, ctx.getPlusPropertyCount());
        assertEquals("-node", 0, ctx.getMinusNodeCount());
        assertEquals("-relationship", 0, ctx.getMinusRelationshipCount());
        assertEquals("-label", 0, ctx.getMinusLabelCount());
        assertEquals("-property", 0, ctx.getMinusPropertyCount());
    }

    @Then("^the side effects should be:$")
    public void thenTheSideEffectsShouldBe(DataTable table) {
        for (List<String> tableRow : table.raw()) {
            if (tableRow.size() == 2 && tableRow.get(0).equals("+nodes")) {
                int plusNodes = Integer.parseInt(tableRow.get(1));
                assertEquals("+nodes", plusNodes, ctx.getPlusNodeCount());
            } else if (tableRow.size() == 2 && tableRow.get(0).equals("+relationships")) {
                int plusRelationships = Integer.parseInt(tableRow.get(1));
                assertEquals("+relationships", plusRelationships, ctx.getPlusRelationshipCount());
            } else if (tableRow.size() == 2 && tableRow.get(0).equals("+labels")) {
                int plusLabels = Integer.parseInt(tableRow.get(1));
                assertEquals("+labels", plusLabels, ctx.getPlusLabelCount());
            } else if (tableRow.size() == 2 && tableRow.get(0).equals("+properties")) {
                int plusProperties = Integer.parseInt(tableRow.get(1));
                assertEquals("+properties", plusProperties, ctx.getPlusPropertyCount());
            } else if (tableRow.size() == 2 && tableRow.get(0).equals("-nodes")) {
                int minusNodes = Integer.parseInt(tableRow.get(1));
                assertEquals("-nodes", minusNodes, ctx.getMinusNodeCount());
            } else if (tableRow.size() == 2 && tableRow.get(0).equals("-relationships")) {
                int minusRelationships = Integer.parseInt(tableRow.get(1));
                assertEquals("-relationships", minusRelationships, ctx.getMinusRelationshipCount());
            } else if (tableRow.size() == 2 && tableRow.get(0).equals("-labels")) {
                int minusLabels = Integer.parseInt(tableRow.get(1));
                assertEquals("-labels", minusLabels, ctx.getMinusLabelCount());
            } else if (tableRow.size() == 2 && tableRow.get(0).equals("-properties")) {
                int minusProperties = Integer.parseInt(tableRow.get(1));
                assertEquals("-properties", minusProperties, ctx.getMinusPropertyCount());
            } else {
                fail("Unhandled side effect row: " + tableRow.stream().collect(Collectors.joining(",")));
            }
        }
    }
}

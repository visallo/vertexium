package org.vertexium.cypher.tck;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        format = "pretty",
        features = "classpath:org/vertexium/cypher/tck/features/MatchAcceptance2.feature",
        glue = "org.vertexium.cypher.glue"
)
public class MatchAcceptance2Test {
}

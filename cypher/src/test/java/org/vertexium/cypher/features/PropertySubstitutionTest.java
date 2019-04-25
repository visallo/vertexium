package org.vertexium.cypher.features;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
    format = "pretty",
    features = "classpath:org/vertexium/cypher/features/PropertySubstitution.feature",
    glue = "org.vertexium.cypher.glue"
)
public class PropertySubstitutionTest {
}

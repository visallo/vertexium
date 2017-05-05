Feature: Vertexium

  Scenario: Support escaping labels
    Given an empty graph
    And having executed:
      """
      CREATE (:`http://vertexium.org/label`)
      """
    When executing query:
      """
      MATCH (n:`http://vertexium.org/label`)
      RETURN length(head(labels(n))) AS len
      """
    Then the result should be:
      | len |
      | 26  |
    And no side effects

  Scenario: Support escaping property names
    Given an empty graph
    And having executed:
      """
      CREATE ({`http://vertexium.org/property`: 'property'})
      """
    When executing query:
      """
      MATCH (n {`http://vertexium.org/property`: 'property'})
      RETURN length(head(keys(n))) AS len
      """
    Then the result should be:
      | len |
      | 29  |
    And no side effects

  Scenario: Support escaping relationship labels
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:`http://vertexium.org/label`]->()
      """
    When executing query:
      """
      MATCH ()-[n]->()
      RETURN length(head(labels(n))) AS len
      """
    Then the result should be:
      | len |
      | 26  |
    And no side effects

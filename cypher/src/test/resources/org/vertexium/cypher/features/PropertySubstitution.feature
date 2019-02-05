Feature: Vertexium

  Scenario: Support normalizing properties in create node
    Given an empty graph
    And having executed:
      """
      CREATE (n {alternativePropertyName: 'a'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.propertyName
      """
    Then the result should be:
      | n.propertyName |
      | 'a'            |

  Scenario: Support normalizing properties in create relationship
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[n {alternativePropertyName: 'a'}]->(:B)
      """
    When executing query:
      """
      MATCH ()-[r]->()
      RETURN r.propertyName
      """
    Then the result should be:
      | r.propertyName |
      | 'a'            |

  Scenario: Support normalizing properties in node first match
    Given an empty graph
    And having executed:
      """
      CREATE ({propertyName: 'a'})
      """
    When executing query:
      """
      MATCH (n {alternativePropertyName: 'a'})
      RETURN n.propertyName
      """
    Then the result should be:
      | n.propertyName  |
      | 'a'             |
    And no side effects

  Scenario: Support normalizing properties in node second match
    Given an empty graph
    And having executed:
      """
      CREATE (a {propertyName: 'a'}), (b {propertyName: 'b'})
      CREATE (a)-->(b)
      """
    When executing query:
      """
      MATCH (a {alternativePropertyName: 'a'})-->(b {alternativePropertyName: 'b'})
      RETURN a.propertyName, b.propertyName
      """
    Then the result should be:
      | a.propertyName | b.propertyName |
      | 'a'            | 'b'            |
    And no side effects

  Scenario: Support normalizing properties in relationship first match
    Given an empty graph
    And having executed:
      """
      CREATE (a)
      CREATE (a)-[n {propertyName: 'a'}]->(a)
      """
    When executing query:
      """
      MATCH (a)
      MATCH (a)-[r {alternativePropertyName: 'a'}]->(a)
      RETURN r.propertyName
      """
    Then the result should be:
      | r.propertyName |
      | 'a'            |
    And no side effects

  Scenario: Support normalizing properties in relationship second match
    Given an empty graph
    And having executed:
      """
      CREATE (a), (b), (c)
      CREATE (a)-[{propertyName: 'a'}]->(b)-[{propertyName: 'b'}]->(c)
      """
    When executing query:
      """
      MATCH ()-[r1 {alternativePropertyName: 'a'}]->()-[r2 {alternativePropertyName: 'b'}]->()
      RETURN r1.propertyName, r2.propertyName
      """
    Then the result should be:
      | r1.propertyName | r2.propertyName |
      | 'a'             | 'b'             |
    And no side effects

  Scenario: Support normalizing properties in return statement
    Given an empty graph
    And having executed:
      """
      CREATE (n {propertyName : 'a'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.alternativePropertyName
      """
    Then the result should be:
      | n.alternativePropertyName |
      | 'a'                       |

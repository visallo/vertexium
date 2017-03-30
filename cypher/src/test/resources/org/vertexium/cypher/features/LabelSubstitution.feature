Feature: Vertexium

  Scenario: Support normalizing labels in create node
    Given an empty graph
    When executing query:
      """
      CREATE (n:alternativeLabelName)
      MATCH (n:labelName)
      RETURN n
      """
    Then the result should be:
      | n            |
      | (:labelName) |

  Scenario: Support normalizing labels in create relationship
    Given an empty graph
    When executing query:
      """
      CREATE (:A)-[n:alternativeLabelName]->(:B)
      MATCH ()-[r]->()
      RETURN r
      """
    Then the result should be:
      | r            |
      | [:labelName] |

  Scenario: Support normalizing labels in node first match
    Given an empty graph
    And having executed:
      """
      CREATE (:labelName)
      """
    When executing query:
      """
      MATCH (n:alternativeLabelName)
      RETURN n
      """
    Then the result should be:
      | n            |
      | (:labelName) |
    And no side effects

  Scenario: Support normalizing labels in node second match
    Given an empty graph
    And having executed:
      """
      CREATE (a:labelName), (b:labelName)
      CREATE (a:labelName)-->(b:labelName)
      """
    When executing query:
      """
      MATCH (a:alternativeLabelName)-->(b:alternativeLabelName)
      RETURN a, b
      """
    Then the result should be:
      | a            | b            |
      | (:labelName) | (:labelName) |
    And no side effects

  Scenario: Support normalizing labels in relationship first match
    Given an empty graph
    And having executed:
      """
      CREATE (a)
      CREATE (a)-[n:labelName]->(a)
      """
    When executing query:
      """
      MATCH (a)
      MATCH (a)-[r:alternativeLabelName]->(a)
      RETURN r
      """
    Then the result should be:
      | r            |
      | [:labelName] |
    And no side effects

  Scenario: Support normalizing labels in relationship second match
    Given an empty graph
    And having executed:
      """
      CREATE (a), (b), (c)
      CREATE (a)-[:labelName]->(b)-[:labelName]->(c)
      """
    When executing query:
      """
      MATCH ()-[r1:alternativeLabelName]->()-[r2:alternativeLabelName]->()
      RETURN r1, r2
      """
    Then the result should be:
      | r1           | r2           |
      | [:labelName] | [:labelName] |
    And no side effects

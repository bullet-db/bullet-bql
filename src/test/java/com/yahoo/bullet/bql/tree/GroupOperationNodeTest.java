package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class GroupOperationNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new GroupOperationNode("AVG", identifier("abc")),
                              new GroupOperationNode("SUM", identifier("abc")),
                              new GroupOperationNode("AVG", identifier("def")));
    }
}

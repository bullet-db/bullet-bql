package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

public class LiteralNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new LiteralNode(5),
                              new LiteralNode("5"));
    }
}

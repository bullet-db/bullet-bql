package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class ParenthesesExpressionNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new ParenthesesExpressionNode(identifier("abc")),
                              new ParenthesesExpressionNode(identifier("def")));
    }
}

package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.expressions.Operation;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class UnaryExpressionNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new UnaryExpressionNode(Operation.SIZE_OF, identifier("abc")),
                              new UnaryExpressionNode(Operation.IS_NULL, identifier("abc")),
                              new UnaryExpressionNode(Operation.SIZE_OF, identifier("def")));
    }
}

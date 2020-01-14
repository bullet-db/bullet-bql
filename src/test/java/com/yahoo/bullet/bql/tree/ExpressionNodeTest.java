/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.Assert;

import java.util.function.Supplier;

public abstract class ExpressionNodeTest {
    /**
     * Helper for testing equals() and hashCode() in classes that extend {@link ExpressionNode}.
     *
     * @param supplier A supplier that constructs the expression to compare to.
     * @param expressions The other expressions to compare to that should be not equal.
     */
    protected void testEqualsAndHashCode(Supplier<ExpressionNode> supplier, ExpressionNode... expressions) {
        ExpressionNode expression = supplier.get();
        Assert.assertEquals(expression, expression);
        Assert.assertEquals(expression.hashCode(), expression.hashCode());

        for (ExpressionNode other : expressions) {
            Assert.assertNotEquals(expression, other);
            Assert.assertNotEquals(expression.hashCode(), other.hashCode());
        }

        ExpressionNode other = supplier.get();
        Assert.assertEquals(expression, other);
        Assert.assertEquals(expression.hashCode(), other.hashCode());

        // coverage
        Assert.assertFalse(expression.equals(null));
    }
}

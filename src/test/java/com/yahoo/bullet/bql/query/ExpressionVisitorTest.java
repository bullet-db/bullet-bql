/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import org.testng.annotations.Test;

public class ExpressionVisitorTest {
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testProcess() {
        // coverage
        new ExpressionVisitor().process(null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitNode() {
        // coverage
        new ExpressionVisitor().visitNode(null, null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitExpression() {
        // coverage
        new ExpressionVisitor().visitExpression(null, null);
    }
}

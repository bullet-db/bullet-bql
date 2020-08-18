/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import org.testng.annotations.Test;

public class ComputableProcessorTest {
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testProcess() {
        // coverage
        new ComputableProcessor().process(null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitExpression() {
        // coverage
        new ComputableProcessor().visitExpression(null, null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitGroupOperation() {
        // coverage
        new ComputableProcessor().visitExpression(null, null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitCountDistinct() {
        // coverage
        new ComputableProcessor().visitExpression(null, null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitDistribution() {
        // coverage
        new ComputableProcessor().visitExpression(null, null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitTopK() {
        // coverage
        new ComputableProcessor().visitExpression(null, null);
    }
}

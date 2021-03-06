/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Collections;

public class TypeCheckerTest {
    @Test
    public void testConstructor() {
        // coverage
        new TypeChecker();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported unary operation: \\+")
    public void testValidateUnaryTypeNotUnary() {
        // coverage
        UnaryExpression expression = Mockito.mock(UnaryExpression.class);
        Mockito.when(expression.getOperand()).thenReturn(expression);
        Mockito.when(expression.getOp()).thenReturn(Operation.ADD);
        TypeChecker.validateUnaryType(null, expression);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported n-ary operation: \\+")
    public void testValidateNAryTypeNotNAry() {
        // coverage
        NAryExpression expression = Mockito.mock(NAryExpression.class);
        Mockito.when(expression.getOperands()).thenReturn(Collections.emptyList());
        Mockito.when(expression.getOp()).thenReturn(Operation.ADD);
        TypeChecker.validateNAryType(null, expression);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported binary operation: NOT")
    public void testValidateBinaryTypeNotBinary() {
        // coverage
        BinaryExpression expression = Mockito.mock(BinaryExpression.class);
        Mockito.when(expression.getLeft()).thenReturn(expression);
        Mockito.when(expression.getRight()).thenReturn(expression);
        Mockito.when(expression.getOp()).thenReturn(Operation.NOT);
        TypeChecker.validateBinaryType(null, expression);
    }
}
